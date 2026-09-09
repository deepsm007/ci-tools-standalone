# Ignore dynamic imports
# pylint: disable=E0401, C0413

import os
import re

import click
import requests
import yaml
from google.api_core.exceptions import NotFound, PermissionDenied
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from google.cloud.secretmanager import SecretPayload

# Test Platform's project in GCP Secret Manager
PROJECT_ID = "openshift-ci-secrets"

# The YAML config which defines which groups have access to which secret collections.
CONFIG_PATH = "https://raw.githubusercontent.com/openshift/release/master/core-services/sync-rover-groups/_config.yaml"

# The string reserved for the index secret associated with each collection.
INDEX_SECRET_NAME = "__index"

# The string reserved for the service account secret associated with each collection.
UPDATER_SA_SECRET_NAME = "updater-service-account"

# Metadata keys attached to secrets at creation time:
# - JIRA_LABEL is a label associating the secret with a specific JIRA project.
JIRA_LABEL = "jira-project"

# - ROTATION_INSTRUCTIONS is an annotation that describes how the secret should be rotated.
ROTATION_INSTRUCTIONS = "rotation-instructions"

# - REQUEST_INFO is an annotation that describes how the secret was originally requested,
#   to help trace who to contact in case of problems.
REQUEST_INFO = "request-information"

# GCP limit for a secret name is 255 chars, 200 should be plenty
# (we must also take into account the collection).
SECRET_NAME_MAX_LENGTH = 200


def ensure_authentication(
    client: secretmanager.SecretManagerServiceClient, collection: str
):
    """
    Ensures that the user is authenticated with Google Cloud.
    If client and collection are not empty, verifies that the user
    has permission to access secrets in a collection.

    Raises:
        click.ClickException: If application default credentials are not found.
    """
    try:
        _, _ = default()
    except DefaultCredentialsError as e:
        raise click.ClickException(
            "Credentials for authenticating into google cloud not found. Run the `login` command to authenticate."
        ) from e

    if client and collection != "":
        index_secret = client.secret_version_path(
            PROJECT_ID, get_index_secret_for_collection(collection), "latest"
        )
        try:
            client.access_secret_version(request={"name": index_secret})
        except PermissionDenied:
            raise click.ClickException(
                f"You don't have permission to access secrets in collection '{collection}'."
            )
        except NotFound:
            raise click.ClickException(f"Collection '{collection}' does not exist.")
        except Exception as e:
            raise click.ClickException(
                f"Failed to access collection '{collection}': {e}"
            )


def validate_collection(_ctx, _param, value):
    if value == "":
        return value

    if "__" in value:
        raise click.BadParameter(
            "Cannot contain double underscores (__), which are reserved as delimiters"
        )

    if value.startswith("_"):
        raise click.BadParameter("Cannot start with an underscore")

    if value.endswith("_"):
        raise click.BadParameter("Cannot end with an underscore")

    if not re.fullmatch(r"[a-z0-9-]([a-z0-9-_]*[a-z0-9-])?|[a-z0-9-]", value):
        invalid_chars = set(re.findall(r"[^a-z0-9-_]", value))
        raise click.BadParameter(
            f"May only contain lowercase letters, numbers, dashes, or an underscore. Invalid characters: {', '.join(repr(c) for c in sorted(invalid_chars))}"
        )
    return value


def validate_secret_name(_ctx, _param, value):
    if value == INDEX_SECRET_NAME:
        raise click.ClickException(
            f"The name '{INDEX_SECRET_NAME}' is reserved for internal use and cannot be used as a secret name."
        )
    if value == UPDATER_SA_SECRET_NAME:
        raise click.ClickException(
            f"The name '{UPDATER_SA_SECRET_NAME}' is reserved for internal use and cannot be used as a secret name."
        )

    if "__" in value:
        raise click.BadParameter(
            "Cannot contain double underscores (__), which are reserved as delimiters"
        )

    if value.startswith("_"):
        raise click.BadParameter("Cannot start with an underscore")

    if value.endswith("_"):
        raise click.BadParameter("Cannot end with an underscore")

    # Allow single char OR multi-char with underscores in middle only
    if not re.fullmatch(
        r"[A-Za-z0-9-]([A-Za-z0-9-_]*[A-Za-z0-9-])?|[A-Za-z0-9-]", value
    ):
        invalid_chars = set(re.findall(r"[^A-Za-z0-9-_]", value))
        raise click.BadParameter(
            f"May only contain letters, numbers, dashes, or an underscore. Invalid characters: {', '.join(repr(c) for c in sorted(invalid_chars))}"
        )
    if len(value) > SECRET_NAME_MAX_LENGTH:
        raise click.BadParameter(
            f"Secret name must be less than {SECRET_NAME_MAX_LENGTH} characters."
        )
    return value


def validate_group_name(_ctx, _param, value):
    """
    Validates group name:
    - Required (cannot be empty)
    - Can contain single underscores (_)
    - Cannot contain double underscores (__)
    - Cannot start or end with underscore
    - Can contain forward slashes (/) for hierarchy
    - Only letters, numbers, dashes, underscores, slashes
    """
    if not value:
        raise click.BadParameter("Group name is required")

    if "__" in value:
        raise click.BadParameter(
            "Cannot contain double underscores (__), which are reserved as delimiters"
        )

    if value.startswith("_"):
        raise click.BadParameter("Cannot start with an underscore")

    if value.startswith("-") and not value.startswith("--dot--"):
        raise click.BadParameter("Cannot start with a hyphen")

    if value.endswith("_"):
        raise click.BadParameter("Cannot end with an underscore")

    # Allow: a-z, A-Z, 0-9, -, _, /
    if not re.fullmatch(r"[a-zA-Z0-9_/-]+", value):
        invalid_chars = set(re.findall(r"[^a-zA-Z0-9_/-]", value))
        raise click.BadParameter(
            f"May only contain letters, numbers, dashes, an underscore, or slashes. "
            f"Invalid characters: {', '.join(repr(c) for c in sorted(invalid_chars))}"
        )

    return value


def validate_path(_ctx, _param, value):
    """
    Validates the secret path in the format 'group/field' or 'group/subgroup/field'.
    """
    if not value:
        raise click.BadParameter("Path is required")

    if "/" not in value:
        raise click.BadParameter(
            "Secret path must include both group and field name, separated by '/'\n"
            "Examples: 'default/token', 'aws/prod/password'"
        )

    parts = value.split("/")
    if any(part == "" for part in parts):
        raise click.BadParameter(
            "Secret path cannot contain empty segments or leading/trailing '/'."
        )
    field = parts[-1]
    group = "/".join(parts[:-1])

    validate_group_name(None, None, group)
    validate_secret_name(None, None, field)

    return value


def get_secret_name(collection: str, path: str) -> str:
    """
    Returns a normalized secret name as it is saved in Google Secret Manager.

    Args:
        collection (str): The name of the secret collection.
        path (str): The group/field path of the secret.

    Returns:
        str: A string in the format "{collection}__{group}__{field}".
        Forward slashes in {group} are converted to double underscores.
    """
    path_normalized = path.replace("/", "__")
    return f"{collection}__{path_normalized}"


def get_index_secret_for_collection(collection: str) -> str:
    return f"{collection}__{INDEX_SECRET_NAME}"


def get_updater_sa_secret_for_collection(collection: str) -> str:
    return f"{collection}__{UPDATER_SA_SECRET_NAME}"


def validate_secret_source(from_file: str, from_literal: str):
    """
    Validates that only one of --from-file or --from-literal is provided.

    Args:
        from_file (str): Path to the file containing secret data.
        from_literal (str): Secret data provided as a string.

    Raises:
        click.BadOptionUsage: If both or neither of the options are provided.
    """
    if from_literal != "" and from_file != "":
        raise click.BadOptionUsage(
            option_name="--from-file",
            message="--from-file and --from-literal cannot both be set at the same time",
        )

    if from_literal == "" and from_file == "":
        raise click.BadOptionUsage(
            option_name="--from-file",
            message="You must provide secret data either as string input or a path to file. See `update --help` for more information.",
        )


def create_payload(from_file: str, from_literal: str) -> bytes:
    """
    Creates a secret payload from either a literal string or a file.

    Args:
        from_file (str): Path to the file containing the secret data.
        from_literal (str): Secret data provided as a string literal.

    Returns:
        bytes: The secret data as bytes.

    Raises:
        click.UsageError: If reading the file fails.
    """
    if from_literal != "":
        return from_literal.encode("UTF-8")

    try:
        # Resolve the path and check if it's readable
        resolved_path = os.path.realpath(from_file)
        if not os.path.isfile(resolved_path):
            raise click.UsageError(
                f"File '{from_file}' does not exist or is not a regular file."
            )

        with open(resolved_path, "rb") as f:
            return f.read()
    except OSError as e:
        raise click.UsageError(f"Failed to read file '{from_file}': {e}") from e
    except click.UsageError:
        raise


def get_group_collections() -> dict[str, list[str]]:
    """
    Returns a dictionary mapping each group to its associated secret collections.

    Returns:
        dict[str, list[str]]: A dictionary where each key is a group name and
        each value is a list of secret collections associated with that group.
    """
    try:
        response = requests.get(CONFIG_PATH, timeout=5)
        response.raise_for_status()
        data = yaml.safe_load(response.text)
    except yaml.YAMLError as e:
        raise click.ClickException(f"Failed to parse configuration: {e}") from e
    except requests.exceptions.RequestException as e:
        raise click.ClickException(
            f"Failed to fetch configuration from GitHub. Please check your network connection and try again: {e}"
        ) from e

    if not isinstance(data, dict):
        raise click.ClickException(
            "Invalid configuration format: expected a top-level YAML mapping."
        )

    result = {}

    for group_name, group_data in data.get("groups", {}).items():
        if not isinstance(group_data, dict):
            continue
        collections = group_data.get("secret_collections", [])
        if collections:
            result[group_name] = sorted(collections)

    return result


def get_collections() -> set[str]:
    """
    Returns a set of all existing collections.

    Returns:
        set[str]: A set containing all secret collections.
    """
    collections_dict = get_group_collections()
    collections_set = set()

    for _, collections in collections_dict.items():
        collections_set.update(collections)  # More efficient than loop

    return collections_set


def check_if_collection_exists(collection: str) -> bool:
    """
    Verifies that the collection exists in the configuration file
    in the release repository (source of truth).

    Args:
        collection (str): Name of the collection to check.

    Returns:
        bool: True if collection is one of the defined collections
        in the configuration file, False otherwise.
    """
    collections_set = get_collections()
    return collection in collections_set


def get_secrets_from_index(
    client: secretmanager.SecretManagerServiceClient, collection: str
) -> list[str]:
    """
    Gets the secrets listed in the index secret of the collection.

    Args:
        client (secretmanager.SecretManagerServiceClient): Secret Manager client.
        collection (str): Name of the collection.

    Returns:
        list[str]: A list of secrets.
    """
    index_secret = client.secret_version_path(
        PROJECT_ID, get_index_secret_for_collection(collection), "latest"
    )
    try:
        response = client.access_secret_version(request={"name": index_secret})
    except PermissionDenied:
        raise click.ClickException(
            f"You don't have permission to access secrets in collection '{collection}'."
        )
    except NotFound:
        raise click.ClickException(f"Collection '{collection}' does not exist.")
    except Exception as e:
        raise click.ClickException(
            f"Failed to list secrets for collection '{collection}': {e}"
        )

    secret_list = []
    try:
        secret_list = yaml.safe_load(response.payload.data.decode("UTF-8"))
    except yaml.YAMLError as e:
        raise click.ClickException(f"Failed to parse the index secret: {e}") from e

    return secret_list if secret_list is not None else []


def destroy_previous_versions(
    client: secretmanager.SecretManagerServiceClient,
    secret_path: str,
    current_version_name: str,
):
    """
    Destroys enabled secret versions to prevent cost accumulation, keeping only the
    current version and the one immediately preceding it as a rollback fallback.

    Args:
        client (secretmanager.SecretManagerServiceClient): Secret Manager client.
        secret_path (str): Full resource path of the secret.
        current_version_name (str): Name of the version to keep.
    """
    previous_versions = []
    for v in client.list_secret_versions(
        request={"parent": secret_path, "filter": "state:ENABLED"}
    ):
        if v.name != current_version_name:
            previous_versions.append(v)

    previous_versions.sort(key=lambda version: version.create_time, reverse=True)
    for v in previous_versions[1:]:
        client.destroy_secret_version(request={"name": v.name})


def update_index_secret(
    client: secretmanager.SecretManagerServiceClient,
    collection: str,
    secret_names: list[str],
):
    """
    Updates the index secret for the collection with a new list of secrets.

    Args:
        client (secretmanager.SecretManagerServiceClient): Secret Manager client.
        collection (str): Name of the collection.
        secret_names (list[str]): A list of the new list of secrets to write into the index.
    """

    name = client.secret_path(PROJECT_ID, get_index_secret_for_collection(collection))
    payload = yaml.safe_dump(sorted(secret_names))
    try:
        new_version = client.add_secret_version(
            parent=name, payload=SecretPayload(data=payload.encode("utf-8"))
        )
    except Exception as e:
        raise click.ClickException(f"Error while updating index: '{e}'.")

    try:
        destroy_previous_versions(client, name, new_version.name)
    except Exception as e:
        click.echo(
            f"Warning: index for collection '{collection}' was updated, but failed to clean up previous versions: {e}",
            err=True,
        )
