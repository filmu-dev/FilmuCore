"""Typed item state-machine primitives for orchestration parity work."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ItemState(StrEnum):
    """Canonical media item states aligned with pipeline progression."""

    REQUESTED = "requested"
    INDEXED = "indexed"
    SCRAPED = "scraped"
    DOWNLOADED = "downloaded"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"
    PARTIAL = "partially_completed"
    ONGOING = "ongoing"
    UNRELEASED = "unreleased"


class ItemEvent(StrEnum):
    """State-machine events accepted by the item transition model."""

    INDEX = "index"
    SCRAPE = "scrape"
    DOWNLOAD = "download"
    COMPLETE = "complete"
    FAIL = "fail"
    RETRY = "retry"
    PARTIAL_COMPLETE = "partial_complete"
    MARK_PARTIAL = "partial_complete"
    MARK_ONGOING = "mark_ongoing"
    MARK_UNRELEASED = "mark_unreleased"


@dataclass(frozen=True)
class ItemTransitionResult:
    """Outcome for a state-machine transition attempt."""

    previous: ItemState
    current: ItemState
    event: ItemEvent


class InvalidItemTransition(ValueError):
    """Raised when an event cannot be applied to the current item state."""


TRANSITIONS: dict[ItemState, dict[ItemEvent, ItemState]] = {
    ItemState.REQUESTED: {
        ItemEvent.INDEX: ItemState.INDEXED,
        ItemEvent.FAIL: ItemState.FAILED,
        ItemEvent.MARK_UNRELEASED: ItemState.UNRELEASED,
    },
    ItemState.INDEXED: {
        ItemEvent.SCRAPE: ItemState.SCRAPED,
        ItemEvent.FAIL: ItemState.FAILED,
        ItemEvent.MARK_UNRELEASED: ItemState.UNRELEASED,
    },
    ItemState.SCRAPED: {
        ItemEvent.DOWNLOAD: ItemState.DOWNLOADED,
        ItemEvent.FAIL: ItemState.FAILED,
        ItemEvent.MARK_UNRELEASED: ItemState.UNRELEASED,
    },
    ItemState.DOWNLOADED: {
        ItemEvent.COMPLETE: ItemState.COMPLETED,
        ItemEvent.FAIL: ItemState.FAILED,
        ItemEvent.PARTIAL_COMPLETE: ItemState.PARTIALLY_COMPLETED,
        ItemEvent.MARK_ONGOING: ItemState.ONGOING,
    },
    ItemState.PARTIALLY_COMPLETED: {
        ItemEvent.INDEX: ItemState.INDEXED,
        ItemEvent.COMPLETE: ItemState.COMPLETED,
    },
    ItemState.ONGOING: {
        ItemEvent.INDEX: ItemState.INDEXED,
        ItemEvent.COMPLETE: ItemState.COMPLETED,
    },
    ItemState.FAILED: {
        ItemEvent.RETRY: ItemState.REQUESTED,
    },
    ItemState.UNRELEASED: {
        ItemEvent.INDEX: ItemState.INDEXED,
    },
    ItemState.COMPLETED: {},
}


@dataclass
class ItemStateMachine:
    """Minimal deterministic state machine for media item lifecycle."""

    state: ItemState = ItemState.REQUESTED

    def apply(self, event: ItemEvent) -> ItemTransitionResult:
        """Apply event transition or raise if current state disallows it."""

        previous = self.state
        next_state = TRANSITIONS.get(previous, {}).get(event)
        if next_state is None:
            raise InvalidItemTransition(
                f"invalid transition: state={previous.value} event={event.value}"
            )

        self.state = next_state
        return ItemTransitionResult(previous=previous, current=next_state, event=event)
