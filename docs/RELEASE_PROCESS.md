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

## Single-Use Branch Rule

Non-release branches are single-use.

- create a fresh branch from current `main`
- open exactly one PR from that branch
- merge or close that PR
- do not reuse that same branch name for later work

This especially applies to `codex/*` branches. Reusing a previously merged branch causes stacked history, duplicated release notes, conflict-heavy PRs, and confusing required-check state because the new PR is no longer a clean delta from current `main`.

The only branch family that is intentionally reused is `release-please--*`, because release automation manages that branch lifecycle itself.

If a PR was opened from a reused branch, the recovery path is:

1. create a fresh branch from current `main`
2. cherry-pick or reapply only the intended commits
3. open a replacement PR from that fresh branch name

## Codex Local-Main Workflow

This repository does not use local feature branches for normal Codex work.

- local `main` is the active DEV branch
- GitHub `main` is the protected integration branch
- new work is done on local `main`
- review happens from a fresh remote branch pushed from local `main`

The required flow is:

1. fetch and sync local `main` to current `origin/main`
2. do the work on local `main`
3. push local `main` to a fresh remote review branch with `npm run branch:codex:push -- <remote-branch-name>`
4. open or update the PR from that remote branch to GitHub `main`

Rules:

- never create a local feature branch for normal Codex work
- never push local `main` directly to `origin/main` for feature work
- never reuse a remote review branch name after its PR was merged or closed
- only reuse the same remote review branch while that PR is still open
- if GitHub `main` moved, sync local `main` first before pushing the review branch again
- after any squash merge or merged release PR, treat every still-open review branch as stale until local `main` has been re-synced and repushed through the guarded publish path

This rule exists because a squash-merged PR leaves the old review branch on a different history line than `main`. Reusing that old remote branch name is what causes repeated `PR Branch Hygiene` failures, merge conflicts on already-touched files, and duplicated PR history.

The repository now enforces this with [`../.github/workflows/pr-branch-hygiene.yml`](../.github/workflows/pr-branch-hygiene.yml).

The local guard path is now:

- `npm run branch:hygiene -- -ReviewBranch <remote-branch-name>` to audit the exact local-main -> remote-review-branch publish path
- `npm run branch:codex:push -- <remote-branch-name>` to run that hygiene check and only then push `HEAD` to the target remote review branch
- `.githooks/pre-push` now passes both the local branch and the destination remote branch name into the hygiene check, so pushes to stale or previously closed/merged review-branch names are blocked before GitHub reports them

The earlier local-preflight path is now:

- create a fresh single-use branch with `npm run branch:codex:new -- <topic>`
- if your long-lived local branch is `dev`, publish to a fresh PR branch with `npm run branch:codex:publish -- <topic>` instead of pushing `dev`
- audit the current branch before opening or updating a PR with `npm run branch:hygiene`
- install the repo-managed pre-push hook once with `npm run hooks:install`

`branch:codex:new` creates a timestamped `codex/<topic>-<utc>` branch from current `origin/main` and refuses to run when tracked changes are present, which prevents accidental reuse of a dirty or already-stacked branch. `branch:codex:publish` is the safer path when local work accumulates on a long-lived branch such as `dev`: it creates a fresh timestamped `codex/<topic>-<utc>` branch from current `origin/main`, applies only the net content diff from the source branch, and commits that clean delta as a single publish commit. `branch:hygiene` checks ahead/behind state against `origin/main` and, when GitHub is reachable, also checks whether the destination review branch name already belongs to an open, merged, or closed PR. `branch:codex:push` is the required guarded push path for local-`main` publishing. `hooks:install` configures the repo's `.githooks/pre-push` hook so branch hygiene runs automatically on every push.

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
- at least one approving review for normal feature PRs
- admin-enforcement for the production branch policy

The release automation itself is now split by privilege:

- [`../.github/workflows/release.yml`](../.github/workflows/release.yml) defaults `GITHUB_TOKEN` to read-only workflow-wide
- the `release` job is the only place that gets write access for PR/tag/release operations
- `release-please` must use `RELEASE_PLEASE_TOKEN`, not `github.token`, because strict required checks are evaluated on the release PR merge commit and must come from a real `pull_request` check suite, not a manual redispatch on the release branch head

This matters because release-please updates made with `GITHUB_TOKEN` can leave the release PR in the broken state GitHub labels as `Expected — Waiting for status to be reported`: the `Verify - ...` jobs exist on the release branch head, but the required merge-commit contexts are missing. Manual `workflow_dispatch` redispatch does not fix that. The repository now fails the release workflow fast unless PAT-backed release automation is enabled, because a loud release-workflow failure is safer than silently opening an unmergeable release PR.

### Release automation credentials

Set both of these in the GitHub repository configuration:

- repository variable `RELEASE_PLEASE_USE_PAT=true`
- repository secret `RELEASE_PLEASE_TOKEN=<fine-grained PAT or GitHub App token with repo contents, pull requests, and workflow-trigger coverage for this repository>`

If either is missing, [`../.github/workflows/release.yml`](../.github/workflows/release.yml) now fails before it can open or update a broken release PR.

Required checks should include at least:

- `Verify - Python Lint / Python Lint`
- `Verify - Python Tests / Python Tests`
- `Verify - Rust Format / Rust Format`
- `Verify - Rust Check / Rust Check`
- `Verify - Rust Tests / Rust Tests`

Add these as required when their runner paths are fully provisioned:

- `PR Branch Hygiene / PR Branch Hygiene`
- `PR Title / Semantic PR Title`
- `Verify - Python Lint / Python Lint`
- `Verify - Python Tests / Python Tests`
- `Verify - Rust Format / Rust Format`
- `Verify - Rust Check / Rust Check`
- `Verify - Rust Tests / Rust Tests`
- `Playback Gate / Playback Gate`
- `Validate Platform Stack / Validate Platform Stack`

The playback gate may stay path-conditional in practice, but the workflow itself is now merged and green. Whether it is already marked required on live protected-branch policy must still be validated from an admin-authenticated host with [`../scripts/check_github_main_policy.ps1`](../scripts/check_github_main_policy.ps1).

The repository now also carries [`../scripts/check_github_main_policy.ps1`](../scripts/check_github_main_policy.ps1) plus package scripts `proof:playback:policy` and `proof:playback:policy:validate` so the exact expected `main` policy can be printed or, when `gh` is installed and authenticated, validated against the live repository settings instead of relying on screenshots or memory.
For the stricter release-candidate posture, the repository also now carries `proof:playback:policy:enterprise` and `proof:playback:policy:enterprise:validate`, which add minimum-review/admin-enforcement expectations and the expected provider/Windows proof-profile contract behind the single playback gate.
The playback-gate workflow now also captures `playback-proof-artifacts/github-main-policy-expected.json` from that same policy checker so branch-protection promotion can key off the canonical expected profile/check names from CI artifacts instead of screenshots or memory.
The playback gate may stay path-conditional in practice, but once the GitHub-hosted runner configuration is provisioned and green it should be marked required for the protected `main` workflow policy described in the playback-gate docs.

The repository now also carries:

- [`../scripts/check_github_main_policy.ps1`](../scripts/check_github_main_policy.ps1)
- `proof:playback:policy`
- `proof:playback:policy:validate`
- `proof:playback:policy:enterprise`
- `proof:playback:policy:enterprise:validate`

Use those instead of screenshots or memory to validate that live branch protection still matches the documented policy.

## Operational Flow

### Safe local `dev` -> PR flow

Use this when your working branch is local-only `dev` and GitHub PRs always target `main`.

One-time setup:

1. Run `npm run hooks:install`.
2. Confirm pushes are now guarded by the local `pre-push` hygiene check.

For each new piece of work:

1. Keep `dev` local only. Do not push `dev` and do not open PRs from `dev`.
2. Rebase `dev` onto `origin/main` before starting a new publish cycle.
3. Do your work on `dev` and commit it locally.
4. When you want the first PR branch, run `npm run branch:codex:publish -- <topic>`.
5. Let that command create a fresh single-use `codex/<topic>-<utc>` branch from current `origin/main`.
6. Push and open the PR from that fresh branch only.

For updates to the same PR:

1. Stay on the PR branch that was created for that PR.
2. Commit review fixes on that same PR branch.
3. Push normally. The local `pre-push` hook will block reused/behind branches before GitHub does.
4. If the PR branch falls behind `origin/main`, rebase it onto current `origin/main` before pushing again.

After the PR is squash-merged:

1. Delete the PR branch locally and remotely.
2. Switch back to `dev`.
3. Rebase `dev` onto the new `origin/main`.
4. Start the next publish cycle by creating a brand-new PR branch with `branch:codex:publish`.

Never do these:

- never push `dev`
- never open more than one PR from the same `codex/*` branch name
- never reuse a previously merged `codex/*` branch name
- never keep opening new PRs from a branch that GitHub has already merged or closed

### Feature delivery

1. Create or push a feature branch.
   Prefer `npm run branch:codex:new -- <topic>` for Codex-driven work so the branch starts from current `origin/main` and gets a unique single-use name.
   If your local working branch is a long-lived `dev`, do not push `dev` itself. Use `npm run branch:codex:publish -- <topic>` to cut a fresh single-use PR branch from current `origin/main`.
2. Open a draft PR against `main`.
3. Set a Conventional Commit PR title immediately.
4. Let CI, review, and follow-up commits happen on the PR.
5. Run `npm run branch:hygiene` before requesting review or converting a draft PR so behind/reuse state is caught locally instead of by the PR gate.
6. When green and approved, use **Squash and merge**.
7. Delete the feature branch after merge, or enable GitHub auto-delete for merged branches.

Do not use the plain merge-commit strategy for release-carrying PRs. If GitHub shows `Merge pull request` instead of `Squash and merge`, the repository merge settings are still misconfigured.

### Local dev branch model

If you keep a local-only `dev` branch as your integration branch:

1. keep `dev` local and rebase it onto `origin/main` as needed
2. do not open PRs from `dev`
3. when you want to publish, run `npm run branch:codex:publish -- <topic>`
4. push and open the PR from the fresh `codex/<topic>-<utc>` branch that command creates

This preserves `dev` as your workspace while keeping GitHub PR branches single-use and clean.

### Release creation

1. The squash merge pushes one Conventional Commit onto `main`.
2. [`../.github/workflows/release.yml`](../.github/workflows/release.yml) runs on that push.
3. Release Please updates or opens the release PR branch using `RELEASE_PLEASE_TOKEN`.
4. Merge the release PR.
5. Release Please tags the repo and publishes the GitHub release.

## What Will Not Work Reliably

- merging with the GitHub `Merge pull request` strategy
- relying on arbitrary branch commit subjects instead of the PR title
- expecting pushes to feature branches to create releases directly
- letting release-please fall back to `GITHUB_TOKEN` while `main` uses strict required status checks

Feature-branch pushes should validate code, not create releases.

## Emergency Version Override

If a release needs to force a specific version instead of the default semantic bump, use Release Please's `Release-As` commit footer on the commit that lands on `main`. That should remain an exception, not the normal flow.
