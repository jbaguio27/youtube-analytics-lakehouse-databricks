"""Run dbt models for the Gold layer."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dbt Gold models.")
    parser.add_argument("--catalog", default=os.getenv("YOUTUBE_ANALYTICS_CATALOG", "youtube_analytics_dev"))
    parser.add_argument("--schema", default=os.getenv("DBT_SCHEMA", "gold"))
    parser.add_argument("--target", default=os.getenv("DBT_TARGET", "dev"))
    parser.add_argument("--project-dir", default=os.getenv("DBT_PROJECT_DIR", "dbt"))
    parser.add_argument(
        "--select",
        default=os.getenv("DBT_RUN_SELECT", "path:models"),
        help="dbt selection expression",
    )
    parser.add_argument("--secret-scope", default=os.getenv("DATABRICKS_SECRET_SCOPE", "youtube-analytics"))
    parser.add_argument("--host-secret-key", default="dbt_host")
    parser.add_argument("--http-path-secret-key", default="dbt_http_path")
    parser.add_argument("--token-secret-key", default="dbt_token")
    return parser.parse_args()


def _resolve_project_dir(project_dir: str) -> Path:
    requested = Path(project_dir)
    candidates: list[Path] = []
    seen: set[str] = set()

    def _add(p: Path) -> None:
        key = str(p)
        if key not in seen:
            seen.add(key)
            candidates.append(p)

    if requested.is_absolute():
        _add(requested)
    else:
        cwd = Path.cwd().resolve()
        # Search from cwd upwards; Databricks often runs inside ingestion/tasks.
        for base in [cwd, *cwd.parents]:
            _add((base / requested).resolve())

        file_value = globals().get("__file__", "")
        if str(file_value).strip():
            script_dir = Path(str(file_value)).resolve().parent
            for base in [script_dir, *script_dir.parents]:
                _add((base / requested).resolve())

    for candidate in candidates:
        if candidate.exists() and (candidate / "dbt_project.yml").exists():
            return candidate

    checked = ", ".join(str(c) for c in candidates) if candidates else str(requested)
    raise FileNotFoundError(
        f"dbt project directory not found or missing dbt_project.yml. Checked: {checked}"
    )


def _secret_or_empty(*, scope: str, key: str) -> str:
    if "dbutils" not in globals():
        return ""
    try:
        value = dbutils.secrets.get(scope=scope, key=key)  # type: ignore[name-defined]
    except Exception:
        return ""
    return str(value).strip()


def _context_or_empty(field: str) -> str:
    if "dbutils" not in globals():
        return ""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]
        if field == "host":
            return str(ctx.apiUrl().get()).strip()
        if field == "token":
            return str(ctx.apiToken().get()).strip()
    except Exception:
        return ""
    return ""


def _resolve_conn_value(*, env_names: list[str], scope: str, secret_key: str) -> str:
    for env_name in env_names:
        env_value = os.getenv(env_name, "").strip()
        if env_value:
            return env_value
    secret_value = _secret_or_empty(scope=scope, key=secret_key)
    if secret_value:
        return secret_value
    return ""


def _build_profiles_dir(
    *,
    target: str,
    catalog: str,
    schema: str,
    secret_scope: str,
    host_secret_key: str,
    http_path_secret_key: str,
    token_secret_key: str,
) -> Path:
    host = _resolve_conn_value(
        env_names=["DBT_DATABRICKS_HOST", "DATABRICKS_HOST"],
        scope=secret_scope,
        secret_key=host_secret_key,
    )
    if not host:
        host = _context_or_empty("host")
    if not host:
        raise RuntimeError(
            "Databricks host is missing. Set DBT_DATABRICKS_HOST or DATABRICKS_HOST, "
            f"or secret {secret_scope}/{host_secret_key}."
        )
    http_path = _resolve_conn_value(
        env_names=["DBT_DATABRICKS_HTTP_PATH", "DATABRICKS_HTTP_PATH"],
        scope=secret_scope,
        secret_key=http_path_secret_key,
    )
    if not http_path:
        raise RuntimeError(
            "Databricks SQL warehouse http_path is missing. Set DBT_DATABRICKS_HTTP_PATH "
            f"or DATABRICKS_HTTP_PATH, or secret {secret_scope}/{http_path_secret_key}."
        )
    token = _resolve_conn_value(
        env_names=["DBT_DATABRICKS_TOKEN", "DATABRICKS_TOKEN"],
        scope=secret_scope,
        secret_key=token_secret_key,
    )
    if not token:
        token = _context_or_empty("token")
    if not token:
        raise RuntimeError(
            "Databricks token is missing. Set DBT_DATABRICKS_TOKEN or DATABRICKS_TOKEN, "
            f"or secret {secret_scope}/{token_secret_key}."
        )
    threads = os.getenv("DBT_THREADS", "4").strip() or "4"

    temp_dir = Path(tempfile.mkdtemp(prefix="dbt_profiles_"))
    profiles_yml = temp_dir / "profiles.yml"
    profiles_yml.write_text(
        (
            "youtube_analytics:\n"
            f"  target: {target}\n"
            "  outputs:\n"
            f"    {target}:\n"
            "      type: databricks\n"
            "      auth_type: pat\n"
            f"      host: {host}\n"
            f"      http_path: {http_path}\n"
            f"      token: {token}\n"
            f"      catalog: {catalog}\n"
            f"      schema: {schema}\n"
            f"      threads: {threads}\n"
        ),
        encoding="utf-8",
    )
    return temp_dir


def _run_command(args: list[str], *, cwd: Path) -> None:
    completed = subprocess.run(args, cwd=str(cwd), check=False, capture_output=True, text=True)
    if completed.stdout:
        print(completed.stdout)
    if completed.returncode != 0:
        if completed.stderr:
            print(completed.stderr)
        stdout_text = (completed.stdout or "").strip()
        stderr_text = (completed.stderr or "").strip()
        details = []
        if stdout_text:
            details.append(f"stdout:\n{stdout_text}")
        if stderr_text:
            details.append(f"stderr:\n{stderr_text}")
        if details:
            raise RuntimeError(
                f"Command failed ({completed.returncode}): {' '.join(args)}\n"
                + "\n\n".join(details)
            )
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(args)}")


def _dbt_args(*, command: str, project_dir: Path, profiles_dir: Path, target: str) -> list[str]:
    return [
        "dbt",
        command,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        target,
    ]


def main() -> None:
    args = _parse_args()
    if shutil.which("dbt") is None:
        raise RuntimeError("dbt CLI is not installed in this runtime. Add dbt-databricks dependency.")

    project_dir = _resolve_project_dir(args.project_dir)
    profiles_dir = _build_profiles_dir(
        target=args.target,
        catalog=args.catalog,
        schema=args.schema,
        secret_scope=args.secret_scope,
        host_secret_key=args.host_secret_key,
        http_path_secret_key=args.http_path_secret_key,
        token_secret_key=args.token_secret_key,
    )
    try:
        _run_command(
            _dbt_args(
                command="debug",
                project_dir=project_dir,
                profiles_dir=profiles_dir,
                target=args.target,
            ),
            cwd=project_dir,
        )
        has_dbt_deps = (project_dir / "packages.yml").exists() or (project_dir / "dependencies.yml").exists()
        if has_dbt_deps:
            _run_command(
                _dbt_args(
                    command="deps",
                    project_dir=project_dir,
                    profiles_dir=profiles_dir,
                    target=args.target,
                ),
                cwd=project_dir,
            )
        else:
            print("Skipping dbt deps: no packages.yml/dependencies.yml found.")
        _run_command(
            [
                *_dbt_args(
                    command="run",
                    project_dir=project_dir,
                    profiles_dir=profiles_dir,
                    target=args.target,
                ),
                "--select",
                args.select,
            ],
            cwd=project_dir,
        )
    finally:
        shutil.rmtree(profiles_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
