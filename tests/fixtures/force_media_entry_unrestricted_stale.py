from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from filmu_py.db.models import ActiveStreamORM, MediaEntryORM
from filmu_py.db.runtime import DatabaseRuntime


async def main(item_id: str, stale_url: str) -> None:
    dsn = os.environ.get("FILMU_PY_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("FILMU_PY_POSTGRES_DSN is not set")

    db = DatabaseRuntime(dsn)
    try:
        async with db.session() as session:
            result = await session.execute(
                select(MediaEntryORM)
                .join(
                    ActiveStreamORM,
                    ActiveStreamORM.media_entry_id == MediaEntryORM.id,
                )
                .where(
                    ActiveStreamORM.item_id == item_id,
                    ActiveStreamORM.role == "direct",
                    MediaEntryORM.item_id == item_id,
                )
                .limit(1)
            )
            entry = result.scalar_one_or_none()

            if entry is None:
                fallback_result = await session.execute(
                    select(MediaEntryORM)
                    .where(
                        MediaEntryORM.item_id == item_id,
                        MediaEntryORM.kind == "remote-direct",
                        MediaEntryORM.unrestricted_url.is_not(None),
                    )
                    .order_by(MediaEntryORM.created_at.desc(), MediaEntryORM.id.desc())
                    .limit(1)
                )
                entry = fallback_result.scalar_one_or_none()

            if entry is None:
                raise SystemExit(
                    f"No mutable direct media entry found for item_id={item_id}"
                )

            entry_id = entry.id
            active_stream_result = await session.execute(
                select(ActiveStreamORM).where(
                    ActiveStreamORM.item_id == item_id,
                    ActiveStreamORM.role == "direct",
                )
            )
            active_stream = active_stream_result.scalar_one_or_none()
            if active_stream is None:
                session.add(
                    ActiveStreamORM(
                        item_id=item_id,
                        media_entry_id=entry_id,
                        role="direct",
                    )
                )
            else:
                active_stream.media_entry_id = entry_id

            entry.unrestricted_url = stale_url
            entry.refresh_state = "ready"
            entry.last_refresh_error = None
            await session.commit()
            print(entry_id)
    finally:
        await db.dispose()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python tests/fixtures/force_media_entry_unrestricted_stale.py <item_id> <stale_url>"
        )
    asyncio.run(main(sys.argv[1], sys.argv[2]))
