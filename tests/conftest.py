import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base


@pytest_asyncio.fixture
async def db_engine():
    # StaticPool: every checkout reuses the *same* connection, which is
    # required for SQLite's `:memory:`, without it, each checkout would
    # open a fresh, separate, empty in-memory database.
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session_factory(db_engine):
    return async_sessionmaker(db_engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def db_session(db_session_factory):
    async with db_session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def fake_redis():
    from fakeredis import aioredis as fakeredis_aioredis

    client = fakeredis_aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def manual_clock():
    """A controllable fake clock for circuit-breaker tests, avoids real
    sleeps, which would make the test suite slow and its timing flaky."""
    state = {"now": 0.0}

    def clock() -> float:
        return state["now"]

    def advance(seconds: float) -> None:
        state["now"] += seconds

    clock.advance = advance
    return clock
