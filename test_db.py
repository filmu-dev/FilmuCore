import asyncio
import os

from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.settings_service import load_settings


async def main():
    dsn = os.getenv("FILMU_PY_POSTGRES_DSN", "postgresql+asyncpg://postgres:postgres@postgres:5432/filmu")
    db = DatabaseRuntime(dsn)
    data = await load_settings(db)
    if data:
        print("API_KEY_IN_DB:", repr(data.get("api_key")))
    else:
        print("NO SETTINGS IN DB")

if __name__ == "__main__":
    asyncio.run(main())
