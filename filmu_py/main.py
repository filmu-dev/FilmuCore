"""ASGI entrypoint for filmu-python."""

from __future__ import annotations

import platform

import uvicorn

from .config import get_settings


def main() -> None:
    """Run the FastAPI service with optional uvloop acceleration."""

    settings = get_settings()

    uvicorn.run(
        "filmu_py.app:create_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        loop="uvloop" if platform.system() != "Windows" else "asyncio",
    )


if __name__ == "__main__":
    main()
