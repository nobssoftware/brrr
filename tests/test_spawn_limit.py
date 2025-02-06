from collections import Counter

import pytest

from brrr import Brrr, SpawnLimitError
from brrr.backends.in_memory import InMemoryByteStore
from brrr.codec import PickleCodec

from .closable_test_queue import ClosableInMemQueue


async def test_spawn_limit_depth():
    b = Brrr()
    b._spawn_limit = 100
    queue = ClosableInMemQueue()
    store = InMemoryByteStore()
    n = 0

    @b.register_task
    async def foo(a: int) -> int:
        nonlocal n
        n += 1
        if a == 0:
            # Prevent false positives from this test by exiting cleanly at some point
            await queue.close()
            return 0
        return await foo(a - 1)

    b.setup(queue, store, store, PickleCodec())
    await b.schedule("foo", (b._spawn_limit + 3,), {})
    with pytest.raises(SpawnLimitError):
        await b.wrrrk()

    assert n == b._spawn_limit


async def test_spawn_limit_breadth_mapped():
    b = Brrr()
    b._spawn_limit = 100
    queue = ClosableInMemQueue()
    store = InMemoryByteStore()
    calls = Counter()

    @b.register_task
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @b.register_task
    async def foo(a: int) -> int:
        calls["foo"] += 1
        # Pass a different argument to avoid the debouncer
        val = sum(await one.map([[x] for x in range(a)]))
        # Remove this if-guard when return calls are debounced.
        if calls["foo"] == a + 1:
            await queue.close()
        return val

    b.setup(queue, store, store, PickleCodec())
    await b.schedule("foo", (b._spawn_limit + 4,), {})
    with pytest.raises(SpawnLimitError):
        await b.wrrrk()

    assert calls["foo"] == 1


async def test_spawn_limit_recoverable():
    b = Brrr()
    b._spawn_limit = 100
    queue = ClosableInMemQueue()
    store = InMemoryByteStore()
    cache = InMemoryByteStore()
    calls = Counter()

    @b.register_task
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @b.register_task
    async def foo(a: int) -> int:
        calls["foo"] += 1
        # Pass a different argument to avoid the debouncer
        val = sum(await one.map([[x] for x in range(a)]))
        # Remove this if-guard when return calls are debounced.
        if calls["foo"] == a + 1:
            await queue.close()
        return val

    b.setup(queue, store, cache, PickleCodec())
    spawn_limit_encountered = False
    n = b._spawn_limit + 1
    await b.schedule("foo", (n,), {})
    while True:
        # Very ugly but this works for testing
        cache.inner = {}
        try:
            await b.wrrrk()
            break
        except SpawnLimitError:
            spawn_limit_encountered = True
    # I expect messages to be left pending as unhandled here, that’s the point:

    assert spawn_limit_encountered
    # Once we debounce parent calls this should be foo=2
    assert calls == Counter(dict(one=n, foo=n + 1))


async def test_spawn_limit_breadth_manual():
    b = Brrr()
    b._spawn_limit = 100
    queue = ClosableInMemQueue()
    store = InMemoryByteStore()
    calls = Counter()

    @b.register_task
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @b.register_task
    async def foo(a: int) -> int:
        calls["foo"] += 1
        total = 0
        for i in range(a):
            # Pass a different argument to avoid the debouncer
            total += await one(i)

        await queue.close()
        return total

    b.setup(queue, store, store, PickleCodec())
    await b.schedule("foo", (b._spawn_limit + 3,), {})
    with pytest.raises(SpawnLimitError):
        await b.wrrrk()

    assert calls == Counter(dict(one=b._spawn_limit / 2, foo=b._spawn_limit / 2))


async def test_spawn_limit_cached():
    b = Brrr()
    b._spawn_limit = 100
    queue = ClosableInMemQueue()
    store = InMemoryByteStore()
    n = 0
    final = None

    @b.register_task
    async def same(a: int) -> int:
        nonlocal n
        n += 1
        return a

    @b.register_task
    async def foo(a: int) -> int:
        val = sum(await same.map([[1]] * a))
        await queue.close()
        nonlocal final
        final = val
        return val

    b.setup(queue, store, store, PickleCodec())
    await b.schedule("foo", (b._spawn_limit + 5,), {})
    await b.wrrrk()
    await queue.join()

    assert n == 1
    assert final == b._spawn_limit + 5
