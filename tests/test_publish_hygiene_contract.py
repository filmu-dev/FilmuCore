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
        "'docs/**'",
        "'README.md'",
        "'CHANGELOG.md'",
        "'QUICK_START.md'",
        "'WINDOWS_README.md'",
        "'LINUX_UNIX_README.md'",
        "'login_page.html'",
    ):
        assert expected in script


def test_pre_push_runs_publish_guard_before_branch_hygiene() -> None:
    hook = (REPO_ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")

    publish_guard = "check_forbidden_publish_paths.ps1"
    branch_hygiene = "check_branch_hygiene.ps1"

    assert publish_guard in hook
    assert branch_hygiene in hook
    assert hook.index(publish_guard) < hook.index(branch_hygiene)
