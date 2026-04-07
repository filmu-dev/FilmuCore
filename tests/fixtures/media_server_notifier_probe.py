import asyncio

from filmu_py.config import UpdatersSettings
from filmu_py.services.media_server import MediaServerNotifier


async def main() -> None:
    settings = UpdatersSettings()
    settings.plex.enabled = True
    settings.plex.url = "http://host.docker.internal:32400"
    settings.plex.token = "local-test-token"
    result = await MediaServerNotifier(settings).notify_all("proof-item")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
