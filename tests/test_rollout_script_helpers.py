import json
import os
import shutil
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
HELPER_SCRIPT = REPO_ROOT / "scripts" / "rollout_script_helpers.ps1"
PWSH = "pwsh"


def _run_pwsh(command: str, *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [PWSH, "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )


def _make_fake_gh(directory: Path) -> None:
    if os.name == "nt":
        gh_path = directory / "gh.cmd"
        gh_path.write_text(
            "@echo off\r\n"
            "if \"%1\"==\"auth\" if \"%2\"==\"status\" exit /b 1\r\n"
            "exit /b 1\r\n",
            encoding="utf-8",
        )
        return

    gh_path = directory / "gh"
    gh_path.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    gh_path.chmod(gh_path.stat().st_mode | stat.S_IXUSR)


@pytest.mark.skipif(shutil.which(PWSH) is None, reason="pwsh is required")
def test_dotenv_parser_normalizes_quoted_and_space_padded_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        textwrap.dedent(
            """
            FILMU_POLICY_ADMIN_TOKEN = "quoted-token"
            GH_TOKEN= 'single-quoted-token'
            GITHUB_TOKEN=   plain-token
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    command = textwrap.dedent(
        f"""
        . "{HELPER_SCRIPT}"
        $dotEnv = Get-DotEnvMap -Path "{env_path}"
        [pscustomobject]@{{
            policy = $dotEnv['FILMU_POLICY_ADMIN_TOKEN']
            gh = $dotEnv['GH_TOKEN']
            github = $dotEnv['GITHUB_TOKEN']
        }} | ConvertTo-Json -Compress
        """
    )

    result = _run_pwsh(command)
    payload = json.loads(result.stdout)

    assert payload == {
        "policy": "quoted-token",
        "gh": "single-quoted-token",
        "github": "plain-token",
    }


@pytest.mark.skipif(shutil.which(PWSH) is None, reason="pwsh is required")
def test_github_policy_validation_requires_authenticated_gh_or_token(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _make_fake_gh(fake_bin)

    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")

    base_env = os.environ.copy()
    base_env["PATH"] = str(fake_bin) + os.pathsep + base_env.get("PATH", "")

    command = textwrap.dedent(
        f"""
        . "{HELPER_SCRIPT}"
        $dotEnv = Get-DotEnvMap -Path "{env_path}"
        (Test-GithubMainPolicyValidationAvailable -DotEnv $dotEnv).ToString().ToLowerInvariant()
        """
    )
    result = _run_pwsh(command, env=base_env)
    assert result.stdout.strip() == "false"

    env_path.write_text('FILMU_POLICY_ADMIN_TOKEN = "token-from-dotenv"\n', encoding="utf-8")
    result = _run_pwsh(command, env=base_env)
    assert result.stdout.strip() == "true"
