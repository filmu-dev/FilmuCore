from filmu_py.db.migrations import should_use_async_engine


def test_should_use_async_engine_detects_asyncpg_urls() -> None:
    assert should_use_async_engine("postgresql+asyncpg://postgres:postgres@postgres:5432/filmu")


def test_should_use_async_engine_leaves_sync_urls_on_sync_path() -> None:
    assert not should_use_async_engine("postgresql://postgres:postgres@postgres:5432/filmu")
    assert not should_use_async_engine("sqlite:///./filmu.db")
