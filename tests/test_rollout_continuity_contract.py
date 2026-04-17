from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_operator_log_pipeline_rollout_tracks_green_streak() -> None:
    script = (REPO_ROOT / 'scripts' / 'check_operator_log_pipeline_rollout.ps1').read_text(
        encoding='utf-8'
    )

    assert 'green_streak' in script
    assert 'history_record_count' in script
    assert 'contract_path' in script



def test_enterprise_rollout_continuity_requires_program_summary_and_operator_streak() -> None:
    script = (REPO_ROOT / 'scripts' / 'check_enterprise_rollout_continuity.ps1').read_text(
        encoding='utf-8'
    )

    assert 'program_summary_present' in script
    assert 'program_environment_count' in script
    assert 'rollout_green_streak' in script
    assert 'program_contract_path_match' in script
    assert 'rollout_contract_path_match' in script


def test_rollout_contract_manifests_exist_and_define_expected_keys() -> None:
    windows_contract = REPO_ROOT / 'ops' / 'rollout' / 'windows-vfs-soak-program.contract.json'
    operator_contract = REPO_ROOT / 'ops' / 'rollout' / 'operator-log-pipeline.contract.json'

    assert windows_contract.is_file()
    assert operator_contract.is_file()

    windows_text = windows_contract.read_text(encoding='utf-8')
    operator_text = operator_contract.read_text(encoding='utf-8')

    assert 'minimum_environment_count' in windows_text
    assert 'required_profiles' in windows_text
    assert 'require_runtime_capture' in windows_text
    assert 'freshness_window_hours' in windows_text
    assert 'required_fields' in windows_text
    assert 'required_fields' in operator_text
    assert 'minimum_green_streak' in operator_text


def test_enterprise_rollout_workflow_uses_github_hosted_pr_gate_and_self_hosted_managed_gate() -> None:
    workflow = (
        REPO_ROOT / '.github' / 'workflows' / 'enterprise-rollout-continuity.yml'
    ).read_text(encoding='utf-8')

    assert "if: github.event_name == 'pull_request'" in workflow
    assert 'runs-on: ubuntu-latest' in workflow
    assert '-AllowBootstrap' in workflow
    assert '-AllowOfflineOperator' in workflow
    assert "if: github.event_name != 'pull_request'" in workflow
    assert 'runs-on: [self-hosted, windows, filmu-vfs]' in workflow
    assert "cancel-in-progress: ${{ github.event_name == 'pull_request' }}" in workflow
