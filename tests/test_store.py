from brrr.backends.in_memory import InMemoryByteStore
import pytest

from abc import ABC, abstractmethod
from brrr.store import (
    AlreadyExists,
    CompareMismatch,
    Memory,
    PickleCodec,
    Store,
    MemKey,
)


class FakeError(Exception):
    pass


class ByteStoreContract(ABC):
    @abstractmethod
    def get_store(self) -> Store:
        """
        This should return a fresh, empty store instance
        """
        raise NotImplementedError()

    async def test_has(self):
        store = self.get_store()

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
        store = self.get_store()

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
        store = self.get_store()

        a1 = MemKey("type-a", "id-1")

        await store.set_new_value(a1, b"value-1")

        assert await store.get(a1) == b"value-1"

        with pytest.raises(CompareMismatch):
            await store.set_new_value(a1, b"value-2")

        await store.set(a1, b"value-2")

        assert await store.get(a1) == b"value-2"

    async def test_compare_and_set(self):
        store = self.get_store()

        a1 = MemKey("type-a", "id-1")

        await store.set(a1, b"value-1")

        with pytest.raises(CompareMismatch):
            await store.compare_and_set(a1, b"value-2", b"value-3")

        await store.compare_and_set(a1, b"value-2", b"value-1")

        assert await store.get(a1) == b"value-2"

    async def test_compare_and_delete(self):
        store = self.get_store()

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


class TestMemory:
    def get_memory(self) -> Memory:
        store = InMemoryByteStore()
        return Memory(store, PickleCodec())

    async def test_call(self):
        memory = self.get_memory()

        with pytest.raises(ValueError):
            await memory.set_call("foo")

        with pytest.raises(KeyError):
            await memory.get_call("non-existent")

        call = memory.make_call("task", (("arg-1", "arg-2"), {"a": 1, "b": 2}))
        assert not await memory.has_call(call)

        await memory.set_call(call)
        assert await memory.has_call(call)
        assert await memory.get_call(call.memo_key) == call

    async def test_value(self):
        memory = self.get_memory()

        assert not await memory.has_value("key")

        await memory.set_value("key", {"test": 1})
        assert await memory.has_value("key")
        assert await memory.get_value("key") == {"test": 1}

        with pytest.raises(AlreadyExists):
            await memory.set_value("key", {"test": 2})

    async def test_pending_returns(self):
        memory = self.get_memory()

        async with memory.with_pending_returns_remove("key") as keys:
            assert keys == set()

        await memory.add_pending_return("key", "p1")
        await memory.add_pending_return("key", "p2")
        await memory.add_pending_return("key", "p2")

        with pytest.raises(FakeError):
            async with memory.with_pending_returns_remove("key") as keys:
                assert keys == {"p1", "p2"}
                raise FakeError()

        async with memory.with_pending_returns_remove("key") as keys:
            assert keys == {"p1", "p2"}

        async with memory.with_pending_returns_remove("key") as keys:
            assert keys == set()


class TestInMemoryByteStore(ByteStoreContract):
    def get_store(self) -> Store:
        return InMemoryByteStore()
