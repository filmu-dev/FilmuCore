# Release Process

## Goal

The repository now treats `release-please` as the only release engine for `main`.

That means the release path must be predictable:

1. work happens on feature branches
2. each branch gets a draft PR
3. CI and review finish on the PR
4. the PR is **squash merged** into `main`
5. the squash-commit subject is a Conventional Commit
6. [`../.github/workflows/release.yml`](../.github/workflows/release.yml) runs on the resulting `main` push
7. Release Please opens or updates the release PR
8. merging that release PR creates the GitHub release and tag

## Why This Policy Exists

Release Please reads commit subjects on `main`.

If `main` receives generic merge commits such as `Merge pull request #4 ...`, or branch commits like `Add semantic alias browse entries to filmuvfs`, the workflow can still run successfully but produce no release PR because those subjects are not release-eligible Conventional Commits.

Squash merge solves that by making the PR title become the commit that lands on `main`.

## Required PR Title Format

PR titles targeting `main` must use Conventional Commit syntax:

- `feat(vfs): add mounted semantic alias browse entries`
- `fix(stream): validate generated local HLS child paths`
- `perf(vfs): reduce duplicate stale-refresh reads`
- `ci(release): enforce semantic PR title gate`
- `docs(release): document squash-only release flow`
- `chore: release 0.3.0`

Allowed types are currently:

- `feat`
- `fix`
- `perf`
- `refactor`
- `build`
- `ci`
- `chore`
- `docs`
- `test`

The PR-title check is enforced by [`../.github/workflows/semantic-pr-title.yml`](../.github/workflows/semantic-pr-title.yml).

## Required GitHub Repository Settings

The repository settings must match this workflow.

### Merge methods

Enable:

- `Squash merge`

Disable:

- `Merge commit`
- `Rebase merge`

This is the critical repo-policy step that makes Release Please deterministic for this project.

### Branch protection for `main`

Require:

- pull requests before merge
- up-to-date branches before merge
- required status checks

Required checks should include at least:

- `Verify - Python Lint / Python Lint`
- `Verify - Python Tests / Python Tests`
- `Verify - Rust Format / Rust Format`
- `Verify - Rust Check / Rust Check`
- `Verify - Rust Tests / Rust Tests`

Add these as required when their runner paths are fully provisioned:

- `PR Title / Semantic PR Title`
- `Verify / Verify - Python Lint`
- `Verify / Verify - Python Tests`
- `Verify / Verify - Rust Format`
- `Verify / Verify - Rust Check`
- `Verify / Verify - Rust Tests`

Add these as required when their runner paths are fully provisioned:

- `Playback Gate / Playback Gate`
- `Validate Platform Stack / Validate Platform Stack`

The playback gate may stay path-conditional in practice, but once the GitHub-hosted runner configuration is provisioned and green it should be marked required for the protected `main` workflow policy described in the playback-gate docs.
The PR-title gate has one bootstrap caveat: do not mark `PR Title / Semantic PR Title` required until after the PR that introduces [`.github/workflows/semantic-pr-title.yml`](../.github/workflows/semantic-pr-title.yml) is merged to `main`, because `pull_request_target` workflows are evaluated from the base branch and cannot report from a workflow that does not yet exist on `main`.

The repository now also carries [`../scripts/check_github_main_policy.ps1`](../scripts/check_github_main_policy.ps1) plus package scripts `proof:playback:policy` and `proof:playback:policy:validate` so the exact expected `main` policy can be printed or, when `gh` is installed and authenticated, validated against the live repository settings instead of relying on screenshots or memory.

## Operational Flow

### Feature delivery

1. Create or push a feature branch.
2. Open a draft PR against `main`.
3. Set a Conventional Commit PR title immediately.
4. Let CI, review, and follow-up commits happen on the PR.
5. When green and approved, use **Squash and merge**.

Do not use the plain merge-commit strategy for release-carrying PRs. If GitHub shows `Merge pull request` instead of `Squash and merge`, the repository merge settings are still misconfigured.

### Release creation

1. The squash merge pushes one Conventional Commit onto `main`.
2. [`../.github/workflows/release.yml`](../.github/workflows/release.yml) runs on that push.
3. Release Please updates or opens the release PR branch.
4. Merge the release PR.
5. Release Please tags the repo and publishes the GitHub release.

## What Will Not Work Reliably

- merging with the GitHub `Merge pull request` strategy
- relying on arbitrary branch commit subjects instead of the PR title
- expecting pushes to feature branches to create releases directly

Feature-branch pushes should validate code, not create releases.

## Emergency Version Override

If a release needs to force a specific version instead of the default semantic bump, use Release Please's `Release-As` commit footer on the commit that lands on `main`. That should remain an exception, not the normal flow.
