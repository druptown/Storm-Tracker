"""Regressietests voor persistente storm-ID-herkoppeling."""
from __future__ import annotations

import pytest


class _MemoryStore:
    def __init__(self) -> None:
        self.saved = None

    async def async_save(self, value) -> None:
        self.saved = value


class _Engine:
    def __init__(self, snapshots) -> None:
        self._snapshots = snapshots

    def export_mcs_history(self):
        return self._snapshots


@pytest.mark.asyncio
async def test_transient_empty_batch_preserves_last_restart_match_state(
    mcs_store_module,
):
    store = mcs_store_module.McsHistoryStore.__new__(
        mcs_store_module.McsHistoryStore
    )
    store._store = _MemoryStore()
    previous = [{"storm_id": "stable-id", "frames": [{"timestamp": 1200}]}]
    store._data = {"51.00,4.00,250": previous}

    await store.async_save_engine("51.00,4.00,250", _Engine([]))

    assert store._data["51.00,4.00,250"] == previous
    assert store._store.saved == {"engines": store._data}


@pytest.mark.asyncio
async def test_new_nonempty_batch_replaces_restart_match_state(
    mcs_store_module,
):
    store = mcs_store_module.McsHistoryStore.__new__(
        mcs_store_module.McsHistoryStore
    )
    store._store = _MemoryStore()
    store._data = {"51.00,4.00,250": [{"storm_id": "old"}]}
    current = [{"storm_id": "continued", "frames": [{"timestamp": 1500}]}]

    await store.async_save_engine("51.00,4.00,250", _Engine(current))

    assert store._data["51.00,4.00,250"] == current


@pytest.mark.asyncio
async def test_explicit_region_removal_still_cleans_restart_state(
    mcs_store_module,
):
    store = mcs_store_module.McsHistoryStore.__new__(
        mcs_store_module.McsHistoryStore
    )
    store._store = _MemoryStore()
    store._data = {"51.00,4.00,250": [{"storm_id": "old"}]}

    await store.async_remove_engine("51.00,4.00,250")

    assert "51.00,4.00,250" not in store._data
