# Ignore dynamic imports
# pylint: disable=E0401, C0413

import click
import proxy_auth


@click.command()
def login():
    """Authenticate to access your secret collections."""
    proxy_auth.login()
    click.echo(
        "login successful. Run `clean` to remove cached credentials.", err=True
    )
