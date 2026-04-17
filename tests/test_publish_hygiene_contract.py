import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_forbidden_publish_paths_blocks_local_tracking_surfaces() -> None:
    script = (REPO_ROOT / "scripts" / "check_forbidden_publish_paths.ps1").read_text(
        encoding="utf-8"
    )

    for expected in (
        "'logs/**'",
        "'ci-artifacts/**'",
        "'playback-proof-artifacts/**'",
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


def test_pre_push_uses_local_source_of_truth_branch_hygiene_mode() -> None:
    hook = (REPO_ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")

    assert "-NoFetch" in hook
    assert "-LocalSourceOfTruth:$true" in hook


def test_verify_python_lint_runs_publish_hygiene_guard() -> None:
    verify_workflow = (REPO_ROOT / ".github" / "workflows" / "verify.yml").read_text(
        encoding="utf-8"
    )

    assert "pwsh -NoProfile -File ./scripts/check_publish_hygiene.ps1" in verify_workflow


def test_publish_hygiene_keeps_release_manifest_forbidden_off_release_branches() -> None:
    script = (REPO_ROOT / "scripts" / "check_publish_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    always_forbidden_match = re.search(
        r"\$alwaysForbiddenPatterns\s*=\s*@\((?P<body>.*?)\)",
        script,
        flags=re.DOTALL,
    )
    assert always_forbidden_match is not None
    always_forbidden = re.findall(r"'([^']+)'", always_forbidden_match.group("body"))

    release_managed_match = re.search(
        r"\$releaseManagedPaths\s*=\s*@\((?P<body>.*?)\)",
        script,
        flags=re.DOTALL,
    )
    assert release_managed_match is not None
    release_managed = re.findall(r"'([^']+)'", release_managed_match.group("body"))

    assert "*.md" not in always_forbidden
    assert "package.json" in release_managed
    assert "pyproject.toml" in release_managed
    assert "rust/filmuvfs/Cargo.toml" in release_managed


def test_publish_hygiene_allows_repo_docs_to_ship_on_feature_branches() -> None:
    script = (REPO_ROOT / "scripts" / "check_publish_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    assert "'logs/**'" in script
    assert "'login_page.html'" in script
    assert "'*.md'" not in script


def test_check_branch_hygiene_defaults_to_local_source_of_truth_mode() -> None:
    script = (REPO_ROOT / "scripts" / "check_branch_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    assert "[bool] $LocalSourceOfTruth = $true" in script
    assert "Local '$sourceLabel' remains authoritative" in script
    assert "fresh single-use remote review branch from the current local source branch" in script


def test_check_branch_hygiene_blocks_merge_commits_and_supports_detached_sources() -> None:
    script = (REPO_ROOT / "scripts" / "check_branch_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    assert "[string] $Commitish = ''" in script
    assert "Detached HEAD source detected" in script
    assert "must keep a linear history relative to '$Remote/$BaseBranch'" in script
    assert "--min-parents=2" in script


def test_check_branch_hygiene_permanently_blocks_stale_review_branch_names() -> None:
    script = (REPO_ROOT / "scripts" / "check_branch_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    assert "$permanentlyBlockedReviewBranches" in script
    assert "'codex/windows-vfs-rollout-20260415'" in script
    assert "permanently blocked for this repository" in script


def test_check_branch_hygiene_requires_semantic_review_branch_prefixes() -> None:
    script = (REPO_ROOT / "scripts" / "check_branch_hygiene.ps1").read_text(
        encoding="utf-8"
    )

    assert "$allowedSemanticReviewBranchPrefixes" in script
    assert "'fix/'" in script
    assert "'feat/'" in script
    assert "must start with a semantic prefix" in script
    assert "Suggested PR title:" in script


def test_push_review_branch_blocks_direct_main_target_and_uses_local_source_of_truth() -> None:
    script = (REPO_ROOT / "scripts" / "push_review_branch.ps1").read_text(
        encoding="utf-8"
    )

    assert "$RemoteBranch -eq $BaseBranch" in script
    assert "dedicated remote review branch" in script
    assert "-NoFetch" in script
    assert "-LocalSourceOfTruth" in script
    assert "Open the PR with title" in script


def test_pr_branch_hygiene_warns_when_branch_is_behind_base() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "pr-branch-hygiene.yml").read_text(
        encoding="utf-8"
    )

    assert "::notice::Branch '$head_ref' differs from '$base_ref' by $behind_by commit(s)." in workflow
    assert "Local is the source of truth for this project" in workflow
    assert "::error::Branch '$head_ref' is behind '$base_ref'" not in workflow


def test_pre_push_and_pr_workflow_block_merge_commits_for_review_branches() -> None:
    hook = (REPO_ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")
    workflow = (REPO_ROOT / ".github" / "workflows" / "pr-branch-hygiene.yml").read_text(
        encoding="utf-8"
    )
    publish_guard = (REPO_ROOT / "scripts" / "check_forbidden_publish_paths.ps1").read_text(
        encoding="utf-8"
    )

    assert 'if [[ "${remote_ref}" != refs/heads/* ]]; then' in hook
    assert "'-LocalSourceOfTruth:$true'" in hook
    assert '-Commitish "${commitish}"' in hook
    assert "merge_commit_count" in workflow
    assert "contains $merge_commit_count merge commit(s)" in workflow
    assert "$RemoteRef -notlike 'refs/heads/*'" in publish_guard
