# Ignore dynamic imports
# pylint: disable=E0401, C0413

import click
import proxy_auth


@click.command()
def login():
    """
    login authenticates to Red Hat SSO; the wrapper script in release repository
    handles host-side Google login.
    """
    proxy_auth.login()
    click.echo(
        "login successful. Run `clean` to remove cached credentials.", err=True
    )
