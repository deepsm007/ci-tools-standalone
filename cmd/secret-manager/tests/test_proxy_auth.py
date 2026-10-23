"""Test Dex login, private caching, and proxy requests."""

import base64
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlencode, urlparse

import click
import requests
from click.testing import CliRunner
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import jwt
from google.auth.crypt import RSASigner

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import proxy_auth
from commands.list import list_secrets as list_command
from commands.list import list_secrets_from_proxy
from commands.login import login as login_command


def signer(key):
    """signer returns a JWT signer for an RSA private key with key ID test-key."""
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return RSASigner.from_string(pem, key_id="test-key")


def encode(value):
    """encode returns the unpadded base64url encoding used by JWT and JWKS."""
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


class ProxyAuthTest(unittest.TestCase):
    """Exercise authentication and listing without production credentials."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        environment = patch.dict(os.environ, {"SM_AUTH_DIR": self.directory.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.signer = signer(self.key)

    def token(self, **overrides):
        """token signs a dummy token; claims overridden with None are omitted."""
        claims = {
            "iss": proxy_auth.ISSUER,
            "aud": proxy_auth.CLIENT_ID,
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "nonce": "nonce",
        }
        claims.update(overrides)
        claims = {name: value for name, value in claims.items() if value is not None}
        return jwt.encode(self.signer, claims).decode()

    def test_save_token(self):
        proxy_auth.save_token("first")
        proxy_auth.save_token("second")
        path = proxy_auth.cache_path()
        self.assertEqual(json.loads(path.read_text()), {"id_token": "second"})
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(list(path.parent.iterdir()), [path])

    def test_cached_token(self):
        now = 1_760_270_400
        for name, overrides, valid in [
            ("valid", {}, True),
            ("before renewal window", {"iat": now - 3419}, True),
            ("at renewal window", {"iat": now - 3420}, False),
            ("future issuance within clock skew", {"iat": now + 60}, True),
            ("future issuance beyond clock skew", {"iat": now + 61}, False),
            ("missing issuance", {"iat": None}, False),
            ("Dex expiry renewal window", {"exp": now + 180}, False),
            ("Dex expiry before age limit", {"exp": now + 181}, True),
            ("wrong issuer", {"iss": "other"}, False),
            ("wrong audience", {"aud": "other"}, False),
            ("public permissions", {"mode": 0o644}, False),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth.time, "time", return_value=now
            ):
                mode = overrides.pop("mode", 0o600)
                token = self.token(**{"iat": now, "exp": now + 86400, **overrides})
                proxy_auth.save_token(token)
                proxy_auth.cache_path().chmod(mode)
                self.assertEqual(proxy_auth.cached_token(), token if valid else None)
        proxy_auth.cache_path().write_text("invalid json")
        proxy_auth.cache_path().chmod(0o600)
        self.assertIsNone(proxy_auth.cached_token())
        proxy_auth.cache_path().unlink()
        self.assertIsNone(proxy_auth.cached_token())

    def test_matches_login_value(self):
        for name, value, expected, matches in [
            ("equal", "nonce", "nonce", True),
            ("different", "wrong", "nonce", False),
            ("empty", "", "", False),
            ("missing", None, "nonce", False),
            ("bytes", b"nonce", "nonce", False),
            ("non-ASCII", "nönce", "nonce", False),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth.secrets,
                "compare_digest",
                wraps=proxy_auth.secrets.compare_digest,
            ) as compare:
                self.assertEqual(
                    proxy_auth.matches_login_value(value, expected), matches
                )
                self.assertEqual(compare.called, name in ("equal", "different"))

    def test_verify_token(self):
        public = self.key.public_key().public_numbers()
        response = Mock()
        response.json.return_value = {
            "keys": [
                {
                    "kid": "test-key",
                    "kty": "RSA",
                    "e": encode(public.e.to_bytes(3, "big")),
                    "n": encode(public.n.to_bytes(256, "big")),
                }
            ]
        }
        now = int(time.time())
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        payload = encode(json.dumps({"iss": proxy_auth.ISSUER}).encode())
        for name, token, nonce, valid in [
            ("valid", self.token(), "nonce", True),
            ("issued within clock skew", self.token(iat=now + 30), "nonce", True),
            ("issued beyond clock skew", self.token(iat=now + 120), "nonce", False),
            ("expired", self.token(exp=1), "nonce", False),
            ("wrong issuer", self.token(iss="other"), "nonce", False),
            ("wrong audience", self.token(aud="other"), "nonce", False),
            (
                "multiple audiences",
                self.token(aud=[proxy_auth.CLIENT_ID, "other"]),
                "nonce",
                False,
            ),
            ("wrong nonce", self.token(), "other", False),
            ("missing nonce", self.token(nonce=None), "nonce", False),
            (
                "signed with another key",
                jwt.encode(signer(other), {"iss": proxy_auth.ISSUER}).decode(),
                "nonce",
                False,
            ),
            (
                "unsigned",
                encode(b'{"alg":"none","kid":"test-key"}') + "." + payload + ".",
                "nonce",
                False,
            ),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth.requests, "get", return_value=response
            ):
                if valid:
                    proxy_auth.verify_token(token, nonce)
                else:
                    with self.assertRaises(ValueError):
                        proxy_auth.verify_token(token, nonce)

    def test_get_token(self):
        for cached in (None, "cached"):
            with self.subTest(cached=cached), patch.object(
                proxy_auth, "cached_token", return_value=cached
            ), patch.object(proxy_auth, "login", return_value="new") as login:
                self.assertEqual(proxy_auth.get_token(), cached or "new")
                self.assertEqual(login.call_count, 0 if cached else 1)

    def test_login(self):
        real_get = requests.get
        real_server = proxy_auth.HTTPServer
        for name, message in [
            ("success", None),
            ("handoff", None),
            ("denied", "denied or cancelled"),
            ("timeout", "timed out"),
            ("bad token", "Could not verify the SSO login token"),
            ("no browser", "Could not open a browser"),
            ("port in use", "local port 8085"),
            ("rejected code", "SSO login was rejected"),
            ("network failure", "network connection"),
            ("save failure", "Could not save the SSO login token (Permission denied)"),
        ]:
            with self.subTest(name=name):
                threads = []
                failures = []

                def browser(url):
                    if name == "no browser":
                        return False
                    query = parse_qs(urlparse(url).query)
                    self.assertEqual(query["code_challenge_method"], ["S256"])
                    self.assertNotIn("client_secret", query)

                    def callback():
                        try:
                            state = query["state"][0]
                            for invalid_state in ("wrong", [state, state]):
                                invalid = real_get(
                                    proxy_auth.REDIRECT_URI
                                    + "?"
                                    + urlencode(
                                        {"state": invalid_state, "code": "bad"},
                                        doseq=True,
                                    ),
                                    timeout=5,
                                )
                                self.assertEqual(invalid.status_code, 400)
                            result = (
                                {"state": state, "error": "access_denied"}
                                if name == "denied"
                                else {"state": state, "code": "dummy-code"}
                            )
                            real_get(
                                proxy_auth.REDIRECT_URI + "?" + urlencode(result),
                                timeout=5,
                            ).raise_for_status()
                        except Exception as error:
                            failures.append(error)

                    if name != "timeout":
                        thread = threading.Thread(target=callback)
                        threads.append(thread)
                        thread.start()
                    return True

                request_path = Path(self.directory.name) / "browser-request"
                real_replace = os.replace

                def handoff(source, destination):
                    real_replace(source, destination)
                    self.assertEqual(request_path.stat().st_mode & 0o777, 0o600)
                    browser(request_path.read_text())

                def fail_for(case, error, default=None):
                    return error if name == case else default

                response = Mock(status_code=400 if name == "rejected code" else 200)
                response.json.return_value = {"id_token": "dummy-token"}
                with patch.dict(
                    os.environ, {"SM_CALLBACK_HOST": "127.0.0.1"}
                ), patch.object(
                    proxy_auth.webbrowser, "open", side_effect=browser
                ), patch.object(
                    proxy_auth, "LOGIN_TIMEOUT", 0.1 if name == "timeout" else 5
                ), patch.object(
                    proxy_auth.requests,
                    "post",
                    return_value=response,
                    side_effect=fail_for("network failure", requests.ConnectionError()),
                ) as exchange, patch.object(
                    proxy_auth,
                    "HTTPServer",
                    side_effect=fail_for(
                        "port in use", OSError(48, "Address in use"), real_server
                    ),
                ), patch.object(
                    proxy_auth, "verify_token", side_effect=fail_for("bad token", ValueError())
                ), patch.object(
                    proxy_auth,
                    "save_token",
                    side_effect=fail_for(
                        "save failure", PermissionError(13, "Permission denied")
                    ),
                ) as save, patch.object(
                    proxy_auth.click, "echo"
                ) as echo, patch.object(
                    proxy_auth.os, "replace", side_effect=handoff
                ), patch.dict(os.environ):
                    os.environ.pop("SM_BROWSER_REQUEST", None)
                    if name == "handoff":
                        os.environ["SM_BROWSER_REQUEST"] = str(request_path)
                    if message is None:
                        self.assertEqual(proxy_auth.login(), "dummy-token")
                        save.assert_called_once_with("dummy-token")
                        data = exchange.call_args.kwargs["data"]
                        self.assertNotIn("client_secret", data)
                        self.assertGreaterEqual(len(data["code_verifier"]), 43)
                    else:
                        with self.assertRaises(click.ClickException) as caught:
                            proxy_auth.login()
                        self.assertIn(message, str(caught.exception))
                        self.assertEqual(save.called, name == "save failure")
                    self.assertEqual(echo.called, name != "port in use")
                for thread in threads:
                    thread.join(timeout=5)
                    self.assertFalse(thread.is_alive())
                self.assertEqual(failures, [])

    def test_list_secrets_from_proxy(self):
        def reply(status, payload=None):
            return Mock(status_code=status, json=Mock(return_value=payload))

        for name, response, expected in [
            ("success", reply(200, {"secrets": ["z/a", "a/b"]}), ["a/b", "z/a"]),
            ("empty", reply(200, {"secrets": []}), []),
            ("unauthorized", reply(401), "Run the `login` command"),
            ("forbidden", reply(403), "recently added to an owning Rover group"),
            ("server error", reply(503), "Listing secrets failed (HTTP 503)"),
            ("network failure", requests.ConnectionError(), "network connection"),
            ("secrets not a list", reply(200, {"secrets": "a/b"}), "invalid response"),
            ("non-string path", reply(200, {"secrets": [1]}), "invalid response"),
            ("missing secrets", reply(200, {}), "invalid response"),
            ("non-object response", reply(200, []), "invalid response"),
            ("outdated wrapper", None, "git pull"),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth, "get_token", return_value="cached"
            ) as get_token, patch(
                "commands.list.requests.get", side_effect=[response]
            ) as request, patch.dict(os.environ):
                if response is None:
                    del os.environ["SM_AUTH_DIR"]
                if isinstance(expected, list):
                    self.assertEqual(list_secrets_from_proxy("collection"), expected)
                else:
                    with self.assertRaises(click.ClickException) as caught:
                        list_secrets_from_proxy("collection")
                    self.assertIn(expected, str(caught.exception))
                if response is None:
                    get_token.assert_not_called()
                    request.assert_not_called()
                    continue
                arguments = request.call_args
                self.assertTrue(arguments.args[0].endswith("/collections/collection/secrets"))
                self.assertEqual(arguments.kwargs["headers"], {"Authorization": "Bearer cached"})
                self.assertFalse(arguments.kwargs["allow_redirects"])

    def test_login_command(self):
        for name, error, exit_code, output in [
            ("success", None, 0, "Login successful."),
            ("SSO failure", click.ClickException("SSO login failed."), 1, "SSO login failed."),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth, "login", side_effect=error
            ) as login:
                result = CliRunner().invoke(login_command)
                self.assertEqual(result.exit_code, exit_code, result.output)
                self.assertIn(output, result.output)
                login.assert_called_once()

    def test_list_command(self):
        for args, expected in [
            (["-c", "collection"], "a/key\n\nz/token\n"),
            (["-c", "collection", "-o", "json"], '[\n  "a/key",\n  "z/token"\n]\n'),
            ([], ""),
        ]:
            with self.subTest(args=args), patch(
                "commands.list.list_secrets_from_proxy",
                return_value=["a/key", "z/token"],
            ) as listing, patch("commands.list.get_group_collections", return_value={}):
                result = CliRunner().invoke(list_command, args)
                self.assertEqual(result.exit_code, 0, result.output)
                self.assertEqual(result.output, expected)
                self.assertEqual(listing.call_count, int(bool(args)))


if __name__ == "__main__":
    unittest.main()
