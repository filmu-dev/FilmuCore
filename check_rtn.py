import asyncio
import logging
from collections import Counter

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from filmu_py.config import Settings
from filmu_py.db.models import MediaItemORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.rtn import RTN, ParsedData
from filmu_py.state.item import ItemState

logging.basicConfig(level=logging.ERROR)

async def main():
    settings = Settings()
    db = DatabaseRuntime(settings.postgres_dsn, echo=False)
    rtn = RTN(settings.ranking)
    
    print("--- RTN DIAGNOSTICS FOR FAILED ITEMS ---")
    
    async with db.session() as session:
        result = await session.execute(
            select(MediaItemORM)
            .options(selectinload(MediaItemORM.streams))
            .where(MediaItemORM.state == ItemState.FAILED.value)
        )
        items = result.scalars().all()
        
        if not items:
            print("No items in FAILED state.")
            
        for item in items:
            streams = item.streams
            if not streams:
                continue
                
            reasons = Counter()
            for stream in streams:
                if not stream.parsed_title:
                    continue
                parsed = ParsedData(
                    raw_title=stream.raw_title,
                    parsed_title=stream.parsed_title,
                    resolution=stream.resolution
                )
                try:
                    ranked = rtn.rank_torrent(parsed, correct_title=item.title)
                    if not ranked.fetch:
                        rejection = ",".join(ranked.failed_checks) or "fetch_failed"
                        reasons[rejection] += 1
                except Exception:
                    pass
                    
            if sum(reasons.values()) > 0:
                print(f"\nItem: {item.title} ({item.id})")
                print(f"Total streams analyzed: {len(streams)}")
                for reason, count in reasons.most_common():
                    print(f"  - {reason}: {count} candidates")

if __name__ == "__main__":
    asyncio.run(main())
