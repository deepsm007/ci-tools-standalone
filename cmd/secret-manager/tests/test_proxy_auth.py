"""Test Dex login, private caching, and proxy requests."""

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


class ProxyAuthTest(unittest.TestCase):
    """Exercise authentication and listing without production credentials."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        environment = patch.dict(os.environ, {"SM_AUTH_DIR": self.directory.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = self.key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        self.signer = RSASigner.from_string(pem, key_id="test-key")

    def token(self, **overrides):
        """token signs a short-lived dummy token with optional claim overrides."""
        claims = {
            "iss": proxy_auth.ISSUER,
            "aud": proxy_auth.CLIENT_ID,
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "nonce": "nonce",
        }
        claims.update(overrides)
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
        for name, overrides, readable, valid in [
            ("valid", {}, False, True),
            ("before renewal window", {"iat": now - 3419}, False, True),
            ("at renewal window", {"iat": now - 3420}, False, False),
            ("within renewal window", {"iat": now - 3500}, False, False),
            ("at maximum age", {"iat": now - 3600}, False, False),
            ("over maximum age", {"iat": now - 7200}, False, False),
            ("future issuance within clock skew", {"iat": now + 60}, False, True),
            ("future issuance beyond clock skew", {"iat": now + 61}, False, False),
            ("missing issuance", {"iat": None}, False, False),
            ("expired", {"exp": now - 1}, False, False),
            ("Dex expiry renewal window", {"exp": now + 180}, False, False),
            ("Dex expiry before age limit", {"exp": now + 181}, False, True),
            ("wrong issuer", {"iss": "other"}, False, False),
            ("wrong audience", {"aud": "other"}, False, False),
            ("public permissions", {}, True, False),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth.time, "time", return_value=now
            ):
                claims = {
                    "iss": proxy_auth.ISSUER,
                    "aud": proxy_auth.CLIENT_ID,
                    "iat": now,
                    "exp": now + 86400,
                }
                claims.update(overrides)
                if claims["iat"] is None:
                    del claims["iat"]
                    token = jwt.encode(self.signer, claims).decode()
                else:
                    token = self.token(**claims)
                proxy_auth.save_token(token)
                if readable:
                    proxy_auth.cache_path().chmod(0o644)
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
            ("different lengths", "n", "nonce", False),
            ("empty", "", "", False),
            ("missing", None, "nonce", False),
            ("list", ["nonce"], "nonce", False),
            ("number", 123, "nonce", False),
            ("bytes", b"nonce", "nonce", False),
            ("non-ASCII", "nönce", "nonce", False),
            ("missing expected", "nonce", None, False),
            ("bytes expected", "nonce", b"nonce", False),
            ("non-ASCII expected", "nonce", "nönce", False),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth.secrets,
                "compare_digest",
                wraps=proxy_auth.secrets.compare_digest,
            ) as compare:
                self.assertEqual(
                    proxy_auth.matches_login_value(value, expected), matches
                )
                if name in ("equal", "different", "different lengths"):
                    compare.assert_called_once_with(value, expected)
                else:
                    compare.assert_not_called()

    def test_verify_token(self):
        import base64

        def number(value):
            return (
                base64.urlsafe_b64encode(
                    value.to_bytes((value.bit_length() + 7) // 8, "big")
                )
                .rstrip(b"=")
                .decode()
            )

        public = self.key.public_key().public_numbers()
        response = Mock()
        response.json.return_value = {
            "keys": [
                {
                    "kid": "test-key",
                    "kty": "RSA",
                    "e": number(public.e),
                    "n": number(public.n),
                }
            ]
        }
        for name, claims, nonce, valid in [
            ("valid", {}, "nonce", True),
            ("issued within clock skew", {"iat": int(time.time()) + 30}, "nonce", True),
            (
                "issued beyond clock skew",
                {"iat": int(time.time()) + 120},
                "nonce",
                False,
            ),
            ("expired", {"exp": 1}, "nonce", False),
            ("wrong issuer", {"iss": "other"}, "nonce", False),
            ("wrong audience", {"aud": "other"}, "nonce", False),
            (
                "multiple audiences",
                {"aud": [proxy_auth.CLIENT_ID, "other"]},
                "nonce",
                False,
            ),
            ("wrong nonce", {}, "other", False),
            ("missing nonce", {"nonce": None}, "nonce", False),
            ("list nonce", {"nonce": ["nonce"]}, "nonce", False),
            ("non-ASCII nonce", {"nonce": "nönce"}, "nonce", False),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth.requests, "get", return_value=response
            ):
                if valid:
                    proxy_auth.verify_token(self.token(**claims), nonce)
                else:
                    with self.assertRaises(ValueError):
                        proxy_auth.verify_token(self.token(**claims), nonce)

    def test_get_token(self):
        for cached in (None, "cached"):
            with self.subTest(cached=cached), patch.object(
                proxy_auth, "cached_token", return_value=cached
            ), patch.object(proxy_auth, "login", return_value="new") as login:
                self.assertEqual(proxy_auth.get_token(), cached or "new")
                self.assertEqual(login.call_count, 0 if cached else 1)

    def test_login(self):
        real_get = requests.get
        for name in (
            "success", "denied", "timeout", "bad token", "no browser", "handoff"
        ):
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
                            for invalid_state in ("wrong", "", "nönce", [state, state]):
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

                response = Mock(status_code=200)
                response.json.return_value = {"id_token": "dummy-token"}
                with patch.dict(
                    os.environ, {"SM_CALLBACK_HOST": "127.0.0.1"}
                ), patch.object(
                    proxy_auth.webbrowser, "open", side_effect=browser
                ), patch.object(
                    proxy_auth, "LOGIN_TIMEOUT", 0.1 if name == "timeout" else 5
                ), patch.object(
                    proxy_auth.requests, "post", return_value=response
                ) as exchange, patch.object(
                    proxy_auth,
                    "verify_token",
                    side_effect=ValueError() if name == "bad token" else None,
                ), patch.object(
                    proxy_auth, "save_token"
                ) as save, patch.object(
                    proxy_auth.click, "echo"
                ) as echo, patch.object(
                    proxy_auth.os, "replace", side_effect=handoff
                ), patch.dict(os.environ):
                    os.environ.pop("SM_BROWSER_REQUEST", None)
                    if name == "handoff":
                        os.environ["SM_BROWSER_REQUEST"] = str(request_path)
                    if name in ("success", "handoff"):
                        self.assertEqual(proxy_auth.login(), "dummy-token")
                        save.assert_called_once_with("dummy-token")
                        data = exchange.call_args.kwargs["data"]
                        self.assertNotIn("client_secret", data)
                        self.assertGreaterEqual(len(data["code_verifier"]), 43)
                    else:
                        with self.assertRaises(click.ClickException):
                            proxy_auth.login()
                        save.assert_not_called()
                    echo.assert_called_once_with(
                        "Opening SSO login in your browser.", err=True
                    )
                for thread in threads:
                    thread.join(timeout=5)
                    self.assertFalse(thread.is_alive())
                self.assertEqual(failures, [])

    def test_list_secrets_from_proxy(self):
        for name, statuses, payload, valid in [
            ("success", [200], {"secrets": ["z/token", "a/key"]}, True),
            ("empty", [200], {"secrets": []}, True),
            ("unauthorized", [401], {}, False),
            ("forbidden", [403], {}, False),
            ("upstream", [503], {}, False),
            ("redirect", [302], {}, False),
            ("malformed", [200], {"secrets": "wrong"}, False),
            ("missing secrets", [200], {}, False),
            ("null response", [200], None, False),
            ("list response", [200], [], False),
            ("string response", [200], "wrong", False),
            ("number response", [200], 123, False),
            ("outdated wrapper", [], {}, False),
        ]:
            with self.subTest(name=name):
                responses = [
                    Mock(status_code=status, json=Mock(return_value=payload))
                    for status in statuses
                ]
                with patch.object(
                    proxy_auth, "get_token", return_value="cached"
                ) as get_token, patch.object(
                    proxy_auth, "login", return_value="fresh"
                ) as login, patch(
                    "commands.list.requests.get", side_effect=responses
                ) as request, patch.dict(os.environ):
                    if not statuses:
                        del os.environ["SM_AUTH_DIR"]
                    if valid:
                        self.assertEqual(
                            list_secrets_from_proxy("collection"),
                            sorted(payload["secrets"]),
                        )
                    else:
                        with self.assertRaises(click.ClickException) as caught:
                            list_secrets_from_proxy("collection")
                        if not statuses:
                            self.assertIn("git pull", str(caught.exception))
                            get_token.assert_not_called()
                            request.assert_not_called()
                            continue
                        if statuses[0] == 401:
                            self.assertIn("sm login", str(caught.exception))
                        if statuses[0] == 200:
                            self.assertEqual(
                                str(caught.exception), "Could not read the response."
                            )
                    login.assert_not_called()
                    request.assert_called_once()
                    self.assertFalse(request.call_args.kwargs["allow_redirects"])

    def test_login_command(self):
        for name, args, error, exit_code, calls in [
            ("success", [], None, 0, 1),
            ("SSO failure", [], click.ClickException("SSO login failed."), 1, 1),
            ("removed option", ["--google"], None, 2, 0),
            ("help", ["--help"], None, 0, 0),
        ]:
            with self.subTest(name=name), patch.object(
                proxy_auth, "login", side_effect=error
            ) as dex:
                result = CliRunner().invoke(login_command, args)
                self.assertEqual(result.exit_code, exit_code, result.output)
                self.assertEqual(dex.call_count, calls)

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
