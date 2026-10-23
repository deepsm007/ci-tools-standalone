# Ignore dynamic imports
# pylint: disable=E0401, C0413

import json

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
    token = proxy_auth.get_token()
    try:
        response = requests.get(
            f"{PROXY_URL}/api/v1/collections/{collection}/secrets",
            headers={"Authorization": "Bearer " + token},
            timeout=30,
            allow_redirects=False,
        )
        if response.status_code == 401:
            raise click.ClickException(
                "Authentication was rejected. Run `sm login` to re-authenticate, then retry the command."
            )
        if response.status_code == 403:
            raise click.ClickException(
                f"You don't have permission to list collection '{collection}'."
            )
        if response.status_code != 200:
            raise click.ClickException(
                f"GSM proxy request failed (HTTP {response.status_code})."
            )
        paths = response.json()["secrets"]
        if not isinstance(paths, list) or not all(
            isinstance(path, str) for path in paths
        ):
            raise ValueError("Invalid paths")
        return sorted(paths)
    except (requests.RequestException, ValueError, KeyError, TypeError) as error:
        raise click.ClickException("Could not read the response.") from error


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
