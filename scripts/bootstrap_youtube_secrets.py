"""Bootstrap YouTube secrets into a Databricks secret scope."""

from __future__ import annotations

import argparse
import json
import subprocess
from typing import Any

from dotenv import load_dotenv
import os


def _run_databricks(args: list[str]) -> subprocess.CompletedProcess[str]:
    cmd = ["databricks", *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Databricks CLI command failed: {' '.join(cmd)}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    return result


def _scope_exists(profile: str, scope: str) -> bool:
    result = _run_databricks(["secrets", "list-scopes", "--profile", profile, "-o", "json"])
    payload: Any = json.loads(result.stdout)
    if isinstance(payload, dict):
        scopes = payload.get("scopes", [])
    elif isinstance(payload, list):
        scopes = payload
    else:
        raise RuntimeError(
            f"Unexpected list-scopes response type: {type(payload).__name__}"
        )
    return any(isinstance(item, dict) and item.get("name") == scope for item in scopes)


def _put_secret(profile: str, scope: str, key: str, value: str) -> None:
    _run_databricks(
        [
            "secrets",
            "put-secret",
            scope,
            key,
            "--string-value",
            value,
            "--profile",
            profile,
        ]
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap YouTube OAuth secrets in Databricks.")
    parser.add_argument(
        "--profile",
        default=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"),
        help="Databricks CLI profile name.",
    )
    parser.add_argument(
        "--scope",
        default=os.getenv("DATABRICKS_SECRET_SCOPE", "youtube-analytics"),
        help="Databricks secret scope name.",
    )
    parser.add_argument("--client-id", default=os.getenv("YOUTUBE_OAUTH_CLIENT_ID", ""))
    parser.add_argument("--client-secret", default=os.getenv("YOUTUBE_OAUTH_CLIENT_SECRET", ""))
    parser.add_argument("--refresh-token", default=os.getenv("YOUTUBE_OAUTH_REFRESH_TOKEN", ""))
    return parser.parse_args()


def _require(name: str, value: str) -> str:
    if not value:
        raise ValueError(f"{name} is required. Provide arg or set it in .env.")
    return value


def main() -> None:
    load_dotenv()
    args = _parse_args()

    profile = _require("profile", args.profile)
    scope = _require("scope", args.scope)
    client_id = _require("client-id", args.client_id)
    client_secret = _require("client-secret", args.client_secret)
    refresh_token = _require("refresh-token", args.refresh_token)

    print(f"Using profile={profile}, scope={scope}")
    if not _scope_exists(profile=profile, scope=scope):
        print(f"Creating secret scope: {scope}")
        _run_databricks(["secrets", "create-scope", scope, "--profile", profile])

    print("Writing secret: yt_client_id")
    _put_secret(profile=profile, scope=scope, key="yt_client_id", value=client_id)
    print("Writing secret: yt_client_secret")
    _put_secret(profile=profile, scope=scope, key="yt_client_secret", value=client_secret)
    print("Writing secret: yt_refresh_token")
    _put_secret(profile=profile, scope=scope, key="yt_refresh_token", value=refresh_token)
    print("Secrets bootstrap completed.")


if __name__ == "__main__":
    main()
