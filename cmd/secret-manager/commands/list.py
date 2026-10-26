# Ignore dynamic imports
# pylint: disable=E0401, C0413

import json
import os

import click
import proxy_auth
import requests
from util import (
    get_group_collections,
    validate_collection,
)

PROXY_URL = "https://gsm-proxy.ci.openshift.org"


@click.command("list")
@click.option(
    "-o",
    "--output",
    type=click.Choice(["json", "text"], case_sensitive=False),
    default="text",
    help="Output format, defaults to plain text but can be set to 'json'. Only applicable when a collection or a group is specified.",
)
@click.option(
    "-c",
    "--collection",
    default="",
    help="Name of the secret collection. Use this option to list all secrets belonging to a specific collection.",
    callback=validate_collection,
)
@click.option(
    "--rover-group",
    default="",
    help="Use this option to list all secret collections for a rover group.",
)
def list_secrets(output: str, collection: str, rover_group: str):
    """
    List secrets.

    Without options: Lists all secret collections that exist.
    With -c {COLLECTION}: Lists all secrets in group/field format.
    With --rover-group {GROUP}: Lists collections accessible to that rover group.
    """
    if collection != "" and rover_group != "":
        raise click.ClickException(
            "--collection and --rover-group cannot both be set at the same time"
        )

    if collection != "":
        list_secrets_for_collection(collection, output)
        return

    collections_dict = get_group_collections()
    if rover_group != "":
        list_collections_for_rover_group(collections_dict, rover_group, output)
    else:
        list_all_collections(collections_dict, output)


def list_all_collections(collections_dict: dict, output: str):
    if output == "json":
        click.echo(json.dumps(collections_dict, indent=2))
    else:
        for group_name, collections in collections_dict.items():
            click.echo(f"{group_name}:")
            for collection in collections:
                click.echo(f"- {collection}")


def list_collections_for_rover_group(
    collections_dict: dict[str, list[str]], rover_group: str, output: str
):
    """Lists collections for a specified rover group."""

    if rover_group and rover_group not in collections_dict:
        click.echo(f"Rover group '{rover_group}' has no secret collections")
        return

    if output == "json":
        click.echo(json.dumps(collections_dict[rover_group], indent=2))
    else:
        for collection in collections_dict[rover_group]:
            click.echo(f"{collection}")


def list_secrets_from_proxy(collection):
    # Only the current wrapper script sets SM_AUTH_DIR and forwards the SSO login callback.
    # This can be removed once we've moved everything to go through the proxy.
    if "SM_AUTH_DIR" not in os.environ:
        raise click.ClickException(
            "Your hack/secret-manager.sh is outdated. Run `git pull` in your openshift/release checkout, then retry."
        )
    token = proxy_auth.get_token()
    try:
        response = requests.get(
            f"{PROXY_URL}/api/v1/collections/{collection}/secrets",
            headers={"Authorization": "Bearer " + token},
            timeout=30,
            allow_redirects=False,
        )
    except requests.RequestException as error:
        raise click.ClickException(
            "Could not reach the secret listing service. Check your network connection, then retry."
        ) from error
    if response.status_code == 401:
        raise click.ClickException(
            "Your SSO login was rejected or has expired. Run the `login` command, then retry."
        )
    if response.status_code == 403:
        raise click.ClickException(
            f"You don't have permission to list collection '{collection}', or it doesn't exist. "
            "To see the collections a Rover group owns, run `list --rover-group <group>`. "
            "If you were recently added to an owning Rover group, run the `login` command and retry."
        )
    if response.status_code != 200:
        raise click.ClickException(
            f"Listing secrets failed (HTTP {response.status_code}). Retry later."
        )
    try:
        paths = response.json()["secrets"]
        if not isinstance(paths, list) or not all(
            isinstance(path, str) for path in paths
        ):
            raise ValueError("Invalid paths")
        return sorted(paths)
    except (ValueError, KeyError, TypeError) as error:
        raise click.ClickException(
            "Received an invalid response from the secret listing service."
        ) from error


def list_secrets_for_collection(collection: str, output: str):
    paths = list_secrets_from_proxy(collection)

    if output == "json":
        click.echo(json.dumps(paths, indent=2))
    else:
        if not paths:
            click.echo("(no secrets)")
            return

        last_top_group = None
        for path in paths:
            top_group = path.split("/")[0] if "/" in path else ""

            if last_top_group is not None and top_group != last_top_group:
                click.echo()

            click.echo(path)
            last_top_group = top_group
