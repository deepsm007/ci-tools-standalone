# Ignore dynamic imports
# pylint: disable=E0401, C0413

import click
import proxy_auth


@click.command()
def login():
    """Authenticate to access your secret collections."""
    proxy_auth.login()
    click.echo(
        "Login successful. To log out, run the `clean` command.", err=True
    )
