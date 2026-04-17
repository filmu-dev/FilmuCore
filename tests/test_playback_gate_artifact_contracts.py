from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_github_main_policy_contract_manifest_exists_and_defines_freshness() -> None:
    contract = REPO_ROOT / "ops" / "rollout" / "github-main-policy.contract.json"

    assert contract.is_file()

    text = contract.read_text(encoding="utf-8")
    assert "schema_version" in text
    assert "artifact_kind" in text
    assert "freshness_window_hours" in text
    assert "required_fields" in text


def test_playback_gate_runner_contract_manifest_exists_and_defines_required_checks() -> None:
    contract = REPO_ROOT / "ops" / "rollout" / "playback-gate-runner-readiness.contract.json"

    assert contract.is_file()

    text = contract.read_text(encoding="utf-8")
    assert "schema_version" in text
    assert "artifact_kind" in text
    assert "freshness_window_hours" in text
    assert "required_checks" in text
    assert "provider_gate_required_checks" in text


def test_media_server_provider_gate_contract_manifest_exists_and_defines_freshness() -> None:
    contract = REPO_ROOT / "ops" / "rollout" / "media-server-provider-parity.contract.json"

    assert contract.is_file()

    text = contract.read_text(encoding="utf-8")
    assert "schema_version" in text
    assert "artifact_kind" in text
    assert "freshness_window_hours" in text
    assert "docker_wsl_evidence_checks" in text
    assert "required_fields" in text


def test_windows_soak_and_native_media_contract_manifests_define_freshness() -> None:
    soak_contract = REPO_ROOT / "ops" / "rollout" / "windows-vfs-soak-program.contract.json"
    media_contract = REPO_ROOT / "ops" / "rollout" / "windows-native-media-proof.contract.json"

    assert soak_contract.is_file()
    assert media_contract.is_file()

    soak_text = soak_contract.read_text(encoding="utf-8")
    media_text = media_contract.read_text(encoding="utf-8")

    assert "schema_version" in soak_text
    assert "artifact_kind" in soak_text
    assert "freshness_window_hours" in soak_text
    assert "required_fields" in soak_text
    assert "schema_version" in media_text
    assert "artifact_kind" in media_text
    assert "freshness_window_hours" in media_text
    assert "required_topology" in media_text
    assert "required_fields" in media_text


def test_github_main_policy_script_emits_normalized_artifact_fields() -> None:
    script = (REPO_ROOT / "scripts" / "check_github_main_policy.ps1").read_text(
        encoding="utf-8"
    )

    assert "ContractPath" in script
    assert "schema_version" in script
    assert "artifact_kind" in script
    assert "captured_at" in script
    assert "expires_at" in script
    assert "failure_reasons" in script
    assert "required_actions" in script


def test_playback_gate_runner_script_emits_contract_fields_and_github_hosted_checks() -> None:
    script = (REPO_ROOT / "scripts" / "check_playback_gate_runner.ps1").read_text(
        encoding="utf-8"
    )

    assert "ContractPath" in script
    assert "schema_version" in script
    assert "artifact_kind" in script
    assert "captured_at" in script
    assert "expires_at" in script
    assert "RUNNER_ENVIRONMENT" in script
    assert "github_hosted_runner" in script
    assert "policy_admin_token" in script
    assert "failure_reasons" in script
    assert "required_actions" in script


def test_media_server_provider_gate_script_emits_contract_and_taxonomy_fields() -> None:
    script = (REPO_ROOT / "scripts" / "run_media_server_proof_gate.ps1").read_text(
        encoding="utf-8"
    )

    assert "ContractPath" in script
    assert "schema_version" in script
    assert "artifact_kind" in script
    assert "captured_at" in script
    assert "expires_at" in script
    assert "failure_reasons" in script
    assert "required_actions" in script
    assert "provider_gate_docker_plex_mount_path_drift" in script
    assert "provider_gate_wsl_host_binary_stale" in script
    assert "provider_gate_entry_id_refresh_identity_missing" in script


def test_windows_soak_program_and_trend_scripts_emit_normalized_artifact_fields() -> None:
    soak_program = (REPO_ROOT / "scripts" / "run_windows_vfs_soak_program.ps1").read_text(
        encoding="utf-8"
    )
    soak_trends = (REPO_ROOT / "scripts" / "check_windows_vfs_soak_trends.ps1").read_text(
        encoding="utf-8"
    )

    assert "schema_version" in soak_program
    assert "artifact_kind" in soak_program
    assert "captured_at" in soak_program
    assert "expires_at" in soak_program
    assert "failure_reasons" in soak_program
    assert "required_actions" in soak_program
    assert "ContractPath" in soak_trends
    assert "schema_version" in soak_trends
    assert "artifact_kind" in soak_trends
    assert "captured_at" in soak_trends
    assert "expires_at" in soak_trends
    assert "failure_reasons" in soak_trends
    assert "required_actions" in soak_trends


def test_windows_native_media_proof_script_emits_contract_and_coverage_fields() -> None:
    script = (REPO_ROOT / "scripts" / "run_windows_media_server_gate.ps1").read_text(
        encoding="utf-8"
    )

    assert "ContractPath" in script
    assert "schema_version" in script
    assert "artifact_kind" in script
    assert "captured_at" in script
    assert "expires_at" in script
    assert "required_topology" in script
    assert "coverage_complete" in script
    assert "failure_reasons" in script
    assert "required_actions" in script


def test_playback_gate_workflow_persists_canonical_runner_and_policy_artifacts() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "playback-gate.yml").read_text(
        encoding="utf-8"
    )

    assert "playback-gate-runner-readiness.json" in workflow
    assert "github-main-policy-current.json" in workflow
    assert "FILMU_POLICY_ADMIN_TOKEN" in workflow
