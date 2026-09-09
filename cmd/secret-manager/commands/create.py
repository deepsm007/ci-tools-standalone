# Ignore dynamic imports
# pylint: disable=E0401, C0413

import re

import click
from google.api_core.exceptions import NotFound, PermissionDenied
from google.cloud import secretmanager
from google.cloud.secretmanager import SecretPayload
from util import (
    JIRA_LABEL,
    PROJECT_ID,
    REQUEST_INFO,
    ROTATION_INSTRUCTIONS,
    check_if_collection_exists,
    create_payload,
    ensure_authentication,
    get_secret_name,
    get_secrets_from_index,
    update_index_secret,
    validate_collection,
    validate_path,
    validate_secret_source,
)

# What a user can type at any metadata prompt to indicate the field does not
# apply. When entered, the corresponding label/annotation is left unset rather
# than storing the literal text.
NOT_APPLICABLE = "none"


@click.command("create")
@click.option(
    "-c",
    "--collection",
    required=True,
    help="The secret collection to store the secret.",
    type=str,
    callback=validate_collection,
)
@click.argument("secret_path", required=True, callback=validate_path, metavar="SECRET_PATH")
@click.option(
    "-f",
    "--from-file",
    default="",
    help="Path to file with secret data.",
    type=click.Path(file_okay=True, dir_okay=False, readable=True),
)
@click.option(
    "-l", "--from-literal", default="", help="Secret data as string input.", type=str
)
def create(collection: str, secret_path: str, from_file: str, from_literal: str):
    """Create a new secret in the specified collection.

    The SECRET_PATH should be in the format 'group/field' where:

    - group: Organizes related secrets (can be hierarchical: 'aws/prod')

    - field: The specific secret name (e.g., 'username', 'password')

    Example: secret-manager create -c my-collection aws/password -l "secret value"
    """
    validate_secret_source(from_file, from_literal)

    if not check_if_collection_exists(collection):
        raise click.ClickException(
            f"Collection '{collection}' doesn't exist.  "
            "To create it, add it to the configuration file in the release repository. "
            "See: https://docs.ci.openshift.org/docs/how-tos/adding-a-new-secret-to-ci/"
        )
    client = secretmanager.SecretManagerServiceClient()
    ensure_authentication(client, collection)

    index_secrets = get_secrets_from_index(client, collection)
    path_normalized = secret_path.replace("/", "__")
    if path_normalized in index_secrets:
        raise click.ClickException(
            f"Secret '{secret_path}' already exists."
        )

    secret_id_normalized = get_secret_name(collection, secret_path)
    try:
        client.get_secret(name=client.secret_path(PROJECT_ID, secret_id_normalized))
        raise click.ClickException(
            f"Secret '{secret_path}' already exists."
        )
    except PermissionDenied as e:
        raise click.ClickException(
            f"Insufficient permissions to check if secret '{secret_path}' exists in collection '{collection}'."
        ) from e
    except NotFound:
        pass  # Secret doesn't exist in GCP - this is good

    # Collect metadata
    click.echo(
        "To help us track ownership and manage secrets effectively, we need to collect a few pieces of info.\n"
        "If a field does not apply to your case, type 'none' to continue.\n"
    )
    labels = prompt_for_labels()
    annotations = prompt_for_annotations()

    try:
        gcp_secret = client.create_secret(
            request={
                "parent": f"projects/{PROJECT_ID}",
                "secret_id": secret_id_normalized,
                "secret": {
                    "replication": {"automatic": {}},
                    "labels": labels,
                    "annotations": annotations,
                },
            }
        )
        client.add_secret_version(
            parent=gcp_secret.name,
            payload=SecretPayload(data=create_payload(from_file, from_literal)),
        )
        update_index_secret(client, collection, index_secrets + [path_normalized])
        click.echo(f"Secret '{secret_path}' created successfully.")
    except Exception as e:
        raise click.ClickException(
            f"Failed to create secret '{secret_path}': {e}. "
            f"If the secret is in an inconsistent state, run 'delete -c {collection} {secret_path}', then try again."
        ) from e


def prompt_for_labels() -> dict[str, str]:
    click.echo(
        "Enter team JIRA project associated with this secret (e.g. 'art' for issues.redhat.com/browse/ART).\n"
        "Test Platform may open tickets in this project to help handle incidents requiring secret rotation."
    )
    while True:
        jira = click.prompt(
            text="Jira project (required)",
            type=str,
        ).strip()
        if jira.lower() == NOT_APPLICABLE:
            return {}
        if is_valid_label_value(jira):
            return {JIRA_LABEL: jira.lower()}
        click.echo(
            "JIRA project label must be 1-63 characters, lowercase alphanumeric or hyphen, and not start or end with a hyphen."
        )


def is_valid_label_value(value: str) -> bool:
    return bool(re.fullmatch(r"[a-z]([-a-z0-9]*[a-z0-9])?", value)) and (
        1 <= len(value) <= 63
    )


def prompt_for_annotations() -> dict[str, str]:
    annotations = {}

    click.echo(
        "\nProvide a short description of how this secret can/will be rotated.\n"
        "This can help future team members support token rotation requirements.\n"
        "Do not include sensitive information."
    )

    rotation = prompt_for_annotation("Rotation info (required)")
    if rotation is not None:
        annotations[ROTATION_INSTRUCTIONS] = rotation

    click.echo(
        "\nProvide a short description of how this secret was originally requested\n"
        "(e.g. links to service now tickets, Jira tickets, documentation).\n"
        "This can help future team members know who to contact in case of problems.\n"
        "Do not include sensitive information."
    )
    request = prompt_for_annotation("Request info (required)")
    if request is not None:
        annotations[REQUEST_INFO] = request
    check_annotations_size(annotations)
    return annotations


def prompt_for_annotation(msg: str) -> str | None:
    """Prompt for an annotation value.

    Returns the entered text, or None if the user typed 'none' to indicate the field does not apply.
    """
    while True:
        value = click.prompt(text=msg, type=str).strip()
        if not value:
            click.echo(f"Input cannot be empty. Please enter a value or '{NOT_APPLICABLE}'.")
            continue
        if value.lower() == NOT_APPLICABLE:
            return None
        return value


def check_annotations_size(annotations: dict):
    size = sum(
        len(key.encode("utf-8")) + len(value.encode("utf-8"))
        for key, value in annotations.items()
    )
    # The total size of annotation keys and values must be less than 16KiB.
    if size >= (16 * 1024):
        raise click.ClickException(
            "Total annotations size exceeds the allowed limit (16KiB)."
        )
