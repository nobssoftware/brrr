from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
import functools
from typing import AsyncIterable

import pytest

from brrr.codec import PickleCodec
from brrr.store import (
    AlreadyExists,
    CompareMismatch,
    Memory,
    Store,
    MemKey,
)


class FakeError(Exception):
    pass


class ByteStoreContract(ABC):
    @abstractmethod
    @asynccontextmanager
    async def with_store(self) -> AsyncIterable[Store]:
        """
        This should return a fresh, empty store instance
        """
        raise NotImplementedError()

    async def test_has(self):
        async with self.with_store() as store:
            a1 = MemKey("type-a", "id-1")
            a2 = MemKey("type-a", "id-2")
            b1 = MemKey("type-b", "id-1")

            assert not await store.has(a1)
            assert not await store.has(a2)
            assert not await store.has(b1)

            await store.set(a1, b"value-1")
            assert await store.has(a1)
            assert not await store.has(a2)
            assert not await store.has(b1)

            await store.set(a2, b"value-2")
            assert await store.has(a1)
            assert await store.has(a2)
            assert not await store.has(b1)

            await store.set(b1, b"value-3")
            assert await store.has(a1)
            assert await store.has(a2)
            assert await store.has(b1)

            await store.delete(a1)
            assert not await store.has(a1)
            assert await store.has(a2)
            assert await store.has(b1)

            await store.delete(a2)
            assert not await store.has(a1)
            assert not await store.has(a2)
            assert await store.has(b1)

            await store.delete(b1)
            assert not await store.has(a1)
            assert not await store.has(a2)
            assert not await store.has(b1)

        async def test_get_set(self):
            store = self.get_store()

            a1 = MemKey("type-a", "id-1")
            a2 = MemKey("type-a", "id-2")
            b1 = MemKey("type-b", "id-1")

            await store.set(a1, b"value-1")
            await store.set(a2, b"value-2")
            await store.set(b1, b"value-3")

            assert await store.get(a1) == b"value-1"
            assert await store.get(a2) == b"value-2"
            assert await store.get(b1) == b"value-3"

            await store.set(a1, b"value-4")
            assert await store.get(a1) == b"value-4"

    async def test_key_error(self):
        async with self.with_store() as store:
            a1 = MemKey("type-a", "id-1")

            with pytest.raises(KeyError):
                await store.get(a1)

            await store.delete(a1)
            with pytest.raises(KeyError):
                await store.get(a1)

            await store.set(a1, b"value-1")

            assert await store.get(a1) == b"value-1"

            await store.delete(a1)
            with pytest.raises(KeyError):
                await store.get(a1)

    async def test_set_new_value(self):
        async with self.with_store() as store:
            a1 = MemKey("type-a", "id-1")

            await store.set_new_value(a1, b"value-1")

            assert await store.get(a1) == b"value-1"

            with pytest.raises(CompareMismatch):
                await store.set_new_value(a1, b"value-2")

            await store.set(a1, b"value-2")

            assert await store.get(a1) == b"value-2"

    async def test_compare_and_set(self):
        async with self.with_store() as store:
            a1 = MemKey("type-a", "id-1")

            await store.set(a1, b"value-1")

            with pytest.raises(CompareMismatch):
                await store.compare_and_set(a1, b"value-2", b"value-3")

            await store.compare_and_set(a1, b"value-2", b"value-1")

            assert await store.get(a1) == b"value-2"

    async def test_compare_and_delete(self):
        async with self.with_store() as store:
            a1 = MemKey("type-a", "id-1")

            with pytest.raises(CompareMismatch):
                await store.compare_and_delete(a1, b"value-2")

            await store.set(a1, b"value-1")

            with pytest.raises(CompareMismatch):
                await store.compare_and_delete(a1, b"value-2")

            assert await store.get(a1) == b"value-1"

            await store.compare_and_delete(a1, b"value-1")

            with pytest.raises(KeyError):
                await store.get(a1)


class MemoryContract(ByteStoreContract):
    @asynccontextmanager
    async def with_memory(self) -> AsyncIterable[Memory]:
        async with self.with_store() as store:
            yield Memory(store, PickleCodec())

    async def test_call(self):
        async with self.with_memory() as memory:
            with pytest.raises(ValueError):
                await memory.set_call("foo")

            with pytest.raises(KeyError):
                await memory.get_call("non-existent")

            call = memory.make_call("task", ("arg-1", "arg-2"), {"a": 1, "b": 2})
            assert not await memory.has_call(call)

            await memory.set_call(call)
            assert await memory.has_call(call)
            assert await memory.get_call(call.memo_key) == call

    async def test_value(self):
        async with self.with_memory() as memory:
            call = memory.make_call("task", (), {})
            assert not await memory.has_value(call)

            await memory.set_value(call, {"test": 1})
            assert await memory.has_value(call)
            assert await memory.get_value(call) == {"test": 1}

            with pytest.raises(AlreadyExists):
                await memory.set_value(call, {"test": 2})

    async def test_pending_returns(self):
        async with self.with_memory() as memory:
            async with memory.with_pending_returns_remove("key") as keys:
                assert keys == set()

            calls = set()

            async def callback(x):
                calls.add(x)

            await memory.add_pending_return("key", "p1", functools.partial(callback, 1))
            await memory.add_pending_return("key", "p2", functools.partial(callback, 2))
            await memory.add_pending_return("key", "p2", functools.partial(callback, 3))

            assert calls == {1}

            with pytest.raises(FakeError):
                async with memory.with_pending_returns_remove("key") as keys:
                    assert keys == {"p1", "p2"}
                    raise FakeError()

            async with memory.with_pending_returns_remove("key") as keys:
                assert keys == {"p1", "p2"}

            async with memory.with_pending_returns_remove("key") as keys:
                assert keys == set()
