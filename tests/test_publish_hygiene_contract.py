from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_forbidden_publish_paths_blocks_docs_and_local_tracking_surfaces() -> None:
    script = (REPO_ROOT / "scripts" / "check_forbidden_publish_paths.ps1").read_text(
        encoding="utf-8"
    )

    for expected in (
        "'logs/**'",
        "'ci-artifacts/**'",
        "'playback-proof-artifacts/**'",
        "'*.md'",
        "'login_page.html'",
        "'.release-please-manifest.json'",
    ):
        assert expected in script


def test_pre_push_runs_publish_guard_before_branch_hygiene() -> None:
    hook = (REPO_ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")

    publish_guard = "check_forbidden_publish_paths.ps1"
    branch_hygiene = "check_branch_hygiene.ps1"

    assert publish_guard in hook
    assert branch_hygiene in hook
    assert hook.index(publish_guard) < hook.index(branch_hygiene)


def test_verify_python_lint_runs_publish_hygiene_guard() -> None:
    verify_workflow = (REPO_ROOT / ".github" / "workflows" / "verify.yml").read_text(
        encoding="utf-8"
    )

    assert "pwsh -NoProfile -File ./scripts/check_publish_hygiene.ps1" in verify_workflow


def test_publish_hygiene_keeps_docs_forbidden_even_on_release_branches() -> None:
    script = (REPO_ROOT / "scripts" / "check_publish_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    assert "$alwaysForbiddenPatterns" in script
    assert "'*.md'" in script
    assert "$releaseManagedPaths" in script
    assert "'package.json'" in script
    assert "'pyproject.toml'" in script
    assert "'rust/filmuvfs/Cargo.toml'" in script
