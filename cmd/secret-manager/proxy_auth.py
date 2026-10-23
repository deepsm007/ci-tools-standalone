"""Authenticate to Dex and cache ID tokens for GSM proxy requests."""

import base64
import hashlib
import json
import os
import secrets
import tempfile
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import click
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import jwt
from google.auth.exceptions import GoogleAuthError

ISSUER = "https://idp.ci.openshift.org"
CLIENT_ID = "gsm-cli"
REDIRECT_URI = "http://127.0.0.1:8085/callback"
LOGIN_TIMEOUT = 180
TOKEN_MAX_AGE = 3600
TOKEN_RELOGIN_MARGIN = 180
# CLOCK_SKEW is how far, in seconds, a token's issue time may be ahead of the local clock.
CLOCK_SKEW = 60


def cache_path():
    """cache_path returns the Dex token path in the private state directory."""

    return (
        Path(
            os.environ.get("SM_AUTH_DIR", "~/.config/openshift-secret-manager")
        ).expanduser()
        / "dex-token.json"
    )


def save_token(token):
    """save_token atomically stores the ID token with owner-only permissions."""

    path = cache_path()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as output:
            json.dump({"id_token": token}, output)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def cached_token():
    """cached_token returns a token until three minutes before its proxy age limit or expiry,
    accepting issue times up to CLOCK_SKEW seconds ahead of the local clock."""

    try:
        path = cache_path()
        if path.is_symlink() or path.stat().st_mode & 0o077:
            return None
        token = json.loads(path.read_text())["id_token"]
        # Cached claims only schedule re-login; the proxy verifies authorization.
        claims = jwt.decode(token, verify=False)
        now = time.time()
        valid_until = min(claims["exp"], claims["iat"] + TOKEN_MAX_AGE)
        if (
            claims.get("iss") == ISSUER
            and claims.get("aud") == CLIENT_ID
            and claims["iat"] <= now + CLOCK_SKEW
            and valid_until > now + TOKEN_RELOGIN_MARGIN
        ):
            return token
    except (OSError, ValueError, KeyError, TypeError, GoogleAuthError):
        pass
    return None


def matches_login_value(value, expected):
    """matches_login_value compares nonempty ASCII login strings in constant time."""

    return (
        isinstance(value, str)
        and isinstance(expected, str)
        and bool(value)
        and bool(expected)
        and value.isascii()
        and expected.isascii()
        and secrets.compare_digest(value, expected)
    )


def verify_token(token, nonce):
    """verify_token checks a newly issued Dex token's signature and login claims."""
    header = jwt.decode_header(token)
    if header.get("alg") != "RS256":
        raise ValueError("Unexpected token algorithm")
    response = requests.get(ISSUER + "/keys", timeout=15, allow_redirects=False)
    response.raise_for_status()
    key = next(
        key for key in response.json()["keys"] if key["kid"] == header.get("kid")
    )

    def number(value):
        return int.from_bytes(
            base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)), "big"
        )

    public_key = rsa.RSAPublicNumbers(number(key["e"]), number(key["n"])).public_key()
    pem = public_key.public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    claims = jwt.decode(
        token, certs=pem, audience=CLIENT_ID, clock_skew_in_seconds=CLOCK_SKEW
    )
    if (
        claims.get("iss") != ISSUER
        or claims.get("aud") != CLIENT_ID
        or not matches_login_value(claims.get("nonce"), nonce)
    ):
        raise ValueError("Unexpected login claims")


def login():
    """login authenticates through Dex with state, nonce, and PKCE S256 and caches the ID token."""

    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    result = {}

    class Callback(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def log_message(self, *_args):
            pass

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            states = query.get("state", [])
            valid = (
                parsed.path == "/callback"
                and len(states) == 1
                and matches_login_value(states[0], state)
            )
            if valid and not result:
                if len(query.get("code", [])) == 1 and "error" not in query:
                    result["code"] = query["code"][0]
                else:
                    result["error"] = True
            self.send_response(200 if valid else 400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(
                b"Return to the terminal to check login status."
                if valid
                else b"Invalid login callback."
            )

    url = (
        ISSUER
        + "/auth?"
        + urlencode(
            {
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "response_type": "code",
                "scope": "openid profile email groups",
                "state": state,
                "nonce": nonce,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
    )
    try:
        server = HTTPServer(
            (os.environ.get("SM_CALLBACK_HOST", "127.0.0.1"), 8085), Callback
        )
    except OSError as error:
        raise click.ClickException(
            "Could not listen on local port 8085 for the SSO login. "
            "Stop the program using that port, then retry."
        ) from error
    try:
        with server:
            server.timeout = 0.5
            click.echo("Opening SSO login in your browser.", err=True)
            browser_request = os.environ.get("SM_BROWSER_REQUEST")
            if browser_request:
                descriptor, temporary = tempfile.mkstemp(
                    dir=Path(browser_request).parent
                )
                try:
                    with os.fdopen(descriptor, "w") as output:
                        output.write(url)
                    os.replace(temporary, browser_request)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)
            elif not webbrowser.open(url):
                raise click.ClickException(
                    "Could not open a browser. Run the `login` command on a machine with a browser."
                )
            deadline = time.monotonic() + LOGIN_TIMEOUT
            while not result and time.monotonic() < deadline:
                server.handle_request()
        if not result:
            raise click.ClickException(
                "SSO login timed out. Run the `login` command to retry."
            )
        if "error" in result:
            raise click.ClickException("SSO login was denied or cancelled.")
        response = requests.post(
            ISSUER + "/token",
            data={
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "code": result["code"],
                "code_verifier": verifier,
            },
            timeout=15,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise click.ClickException(
                "The SSO login was rejected. Run the `login` command to retry."
            )
        token = response.json()["id_token"]
        try:
            verify_token(token, nonce)
        except (ValueError, KeyError, TypeError, StopIteration, GoogleAuthError) as error:
            raise click.ClickException(
                "Could not verify the SSO login token. Try running the `login` command again."
            ) from error
        try:
            save_token(token)
        except OSError as error:
            raise click.ClickException(
                f"Could not save the SSO login token ({error.strerror or error}). "
                "Run the `clean` command, then the `login` command."
            ) from error
        return token
    except requests.RequestException as error:
        raise click.ClickException(
            "Could not reach the SSO login service. Check your network connection, then retry."
        ) from error
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise click.ClickException(
            "SSO login failed. Run the `login` command to retry."
        ) from error


def get_token():
    """get_token returns the cached ID token or starts browser login."""

    return cached_token() or login()
