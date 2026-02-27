"""One-time local OAuth flow to retrieve a YouTube refresh token."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import threading
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from dotenv import load_dotenv

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


@dataclass
class OAuthCallbackResult:
    code: str = ""
    state: str = ""
    error: str = ""
    error_description: str = ""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one-time local OAuth flow and print refresh token."
    )
    parser.add_argument(
        "--client-id",
        required=False,
        default=os.getenv("YOUTUBE_OAUTH_CLIENT_ID", ""),
        help="Google OAuth client id.",
    )
    parser.add_argument(
        "--client-secret",
        required=False,
        default=os.getenv("YOUTUBE_OAUTH_CLIENT_SECRET", ""),
        help="Google OAuth client secret.",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("YOUTUBE_OAUTH_REDIRECT_HOST", "127.0.0.1"),
        help="Local callback host configured in Google OAuth app.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("YOUTUBE_OAUTH_REDIRECT_PORT", "8765")),
        help="Local callback port configured in Google OAuth app.",
    )
    parser.add_argument(
        "--scopes",
        nargs="+",
        default=DEFAULT_SCOPES,
        help="OAuth scopes (space-separated list).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=180,
        help="Time to wait for OAuth callback.",
    )
    return parser


def _start_callback_server(host: str, port: int, timeout_seconds: int) -> tuple[OAuthCallbackResult, threading.Thread]:
    result = OAuthCallbackResult()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib API shape
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            result.code = qs.get("code", [""])[0]
            result.state = qs.get("state", [""])[0]
            result.error = qs.get("error", [""])[0]
            result.error_description = qs.get("error_description", [""])[0]

            if result.error:
                message = "OAuth failed. You can close this window and check terminal output."
            else:
                message = "OAuth completed. You can close this window and return to terminal."

            body = message.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    httpd = HTTPServer((host, port), CallbackHandler)
    httpd.timeout = timeout_seconds

    def run_server() -> None:
        httpd.handle_request()
        httpd.server_close()

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    return result, thread


def _exchange_code_for_tokens(
    *,
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str,
) -> dict[str, Any]:
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
    }
    response = requests.post(TOKEN_URL, data=data, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    load_dotenv()
    args = _build_parser().parse_args()
    if not args.client_id:
        raise ValueError("Missing client id. Set YOUTUBE_OAUTH_CLIENT_ID or pass --client-id.")
    if not args.client_secret:
        raise ValueError(
            "Missing client secret. Set YOUTUBE_OAUTH_CLIENT_SECRET or pass --client-secret."
        )
    redirect_uri = f"http://{args.host}:{args.port}"
    state = secrets.token_urlsafe(24)

    callback_result, callback_thread = _start_callback_server(
        host=args.host, port=args.port, timeout_seconds=args.timeout_seconds
    )

    auth_params = {
        "client_id": args.client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(args.scopes),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    consent_url = f"{AUTH_URL}?{urlencode(auth_params)}"

    print("\nOpen this URL if browser does not launch automatically:\n")
    print(consent_url)
    print()
    webbrowser.open(consent_url, new=1)

    callback_thread.join(timeout=args.timeout_seconds + 5)
    if callback_thread.is_alive():
        raise TimeoutError(
            f"Timed out waiting for OAuth callback on {redirect_uri}. "
            "Verify redirect URI is configured in Google Cloud Console."
        )

    if callback_result.error:
        raise RuntimeError(
            f"OAuth authorization failed: {callback_result.error} - "
            f"{callback_result.error_description}"
        )
    if not callback_result.code:
        raise RuntimeError(
            "No authorization code received. Verify redirect URI, then retry."
        )
    if callback_result.state != state:
        raise RuntimeError("OAuth state mismatch. Aborting for safety.")

    token_payload = _exchange_code_for_tokens(
        client_id=args.client_id,
        client_secret=args.client_secret,
        code=callback_result.code,
        redirect_uri=redirect_uri,
    )
    refresh_token = token_payload.get("refresh_token", "")
    if not refresh_token:
        raise RuntimeError(
            "No refresh_token returned. Re-run and ensure prompt=consent and offline access."
        )

    output = {
        "yt_client_id": args.client_id,
        "yt_client_secret": args.client_secret,
        "yt_refresh_token": refresh_token,
        "scopes": args.scopes,
        "token_response_subset": {
            "token_type": token_payload.get("token_type"),
            "scope": token_payload.get("scope"),
            "expires_in": token_payload.get("expires_in"),
        },
    }
    print("OAuth token exchange succeeded.\n")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
