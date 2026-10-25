import asyncio

import pytest

from app.resilience.cache import TwoTierCache

pytestmark = pytest.mark.asyncio


async def test_miss_then_populates_both_tiers(fake_redis):
    cache = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=30, l2_ttl_seconds=30)

    assert await cache.get("k1") is None
    await cache.set("k1", {"v": 1})

    assert await cache.get("k1") == {"v": 1}
    # Confirm L2 (Redis) was actually written, not just L1.
    assert await fake_redis.get("t:k1") is not None


async def test_l1_hit_does_not_touch_redis(fake_redis):
    cache = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=30, l2_ttl_seconds=30)
    await cache.set("k1", {"v": 1})

    # Delete from Redis directly, if get() still returns a value, it came from L1.
    await fake_redis.delete("t:k1")
    assert await cache.get("k1") == {"v": 1}


async def test_l2_hit_backfills_l1(fake_redis):
    cache_a = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=30, l2_ttl_seconds=30)
    cache_b = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=30, l2_ttl_seconds=30)

    await cache_a.set("k1", {"v": 1})  # separate instance, e.g. a different replica
    assert await cache_b.get("k1") == {"v": 1}  # served from L2, backfilling cache_b's L1

    await fake_redis.delete("t:k1")
    assert await cache_b.get("k1") == {"v": 1}  # now served from cache_b's own L1


async def test_invalidate_clears_both_tiers(fake_redis):
    cache = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=30, l2_ttl_seconds=30)
    await cache.set("k1", {"v": 1})

    await cache.invalidate("k1")

    assert await cache.get("k1") is None
    assert await fake_redis.get("t:k1") is None


async def test_l1_entry_expires_after_ttl(fake_redis):
    cache = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=0.05, l2_ttl_seconds=30)
    await cache.set("k1", {"v": 1})
    await fake_redis.delete("t:k1")  # only L1 can serve it now

    assert await cache.get("k1") == {"v": 1}
    await asyncio.sleep(0.1)
    assert await cache.get("k1") is None  # L1 expired, L2 was already empty


async def test_get_or_set_only_calls_loader_on_miss(fake_redis):
    cache = TwoTierCache(fake_redis, namespace="t", l1_max_size=10, l1_ttl_seconds=30, l2_ttl_seconds=30)
    calls = {"n": 0}

    async def loader():
        calls["n"] += 1
        return {"v": calls["n"]}

    first = await cache.get_or_set("k1", loader)
    second = await cache.get_or_set("k1", loader)

    assert first == second == {"v": 1}
    assert calls["n"] == 1
