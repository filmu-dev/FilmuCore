from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_operator_log_pipeline_rollout_tracks_green_streak() -> None:
    script = (REPO_ROOT / 'scripts' / 'check_operator_log_pipeline_rollout.ps1').read_text(
        encoding='utf-8'
    )

    assert 'green_streak' in script
    assert 'history_record_count' in script



def test_enterprise_rollout_continuity_requires_program_summary_and_operator_streak() -> None:
    script = (REPO_ROOT / 'scripts' / 'check_enterprise_rollout_continuity.ps1').read_text(
        encoding='utf-8'
    )

    assert 'program_summary_present' in script
    assert 'program_environment_count' in script
    assert 'rollout_green_streak' in script
