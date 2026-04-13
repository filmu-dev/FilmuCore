"""Stable worker-stage job id and idempotency-key helpers."""

from __future__ import annotations


def worker_stage_idempotency_key(
    stage_name: str,
    item_id: str,
    *,
    discriminator: str | None = None,
) -> str:
    """Return a stable idempotency key for one stage/item combination."""

    if discriminator is None or discriminator == "":
        return f"{stage_name}:{item_id}"
    return f"{stage_name}:{item_id}:{discriminator}"


def index_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for index-stage processing."""

    return f"index-item:{item_id}"


def parse_scrape_results_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for parse-scrape-results processing."""

    return f"parse-scrape-results:{item_id}"


def process_scraped_item_job_id(item_id: str) -> str:
    """Backward-compatible alias for the parse-scrape-results stage identifier."""

    return parse_scrape_results_job_id(item_id)


def rank_streams_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for rank-streams processing."""

    return f"rank-streams:{item_id}"


def scrape_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for scrape-stage processing."""

    return f"scrape-item:{item_id}"


def debrid_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for debrid-stage processing."""

    return f"debrid-item:{item_id}"


def finalize_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for finalize-stage processing."""

    return f"finalize-item:{item_id}"


def refresh_direct_playback_link_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for queued direct-play refresh work."""

    return f"refresh-direct-playback:{item_id}"


def refresh_selected_hls_failed_lease_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for queued failed-HLS refresh work."""

    return f"refresh-selected-hls-failed-lease:{item_id}"


def refresh_selected_hls_restricted_fallback_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for queued restricted-fallback refresh work."""

    return f"refresh-selected-hls-restricted-fallback:{item_id}"
