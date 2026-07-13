"""Storm Tracker V3 — tests/test_storm.py v0.1.0

Directe tests voor engine/storm.py (de Storm-dataclass zelf), los van
de StormEngine die hem aanstuurt.
"""
from __future__ import annotations

import time

import pytest


def _make_strike(ts, lat, lon):
    class _S:
        pass
    s = _S()
    s.timestamp, s.lat, s.lon = ts, lat, lon
    return s


def test_new_storm_has_default_dormant_false(storm_module):
    storm = storm_module.Storm(centroid_lat=51.0, centroid_lon=4.0)
    assert storm.is_dormant is False
    assert storm.strike_count == 0
    assert storm.storm_id  # niet leeg


def test_two_storms_have_unique_ids(storm_module):
    s1 = storm_module.Storm()
    s2 = storm_module.Storm()
    assert s1.storm_id != s2.storm_id


def test_add_strikes_updates_count_and_clears_dormant(storm_module):
    storm = storm_module.Storm()
    storm.is_dormant = True
    now = time.time()
    strikes = [_make_strike(now, 51.0, 4.0), _make_strike(now, 51.01, 4.01)]

    storm.add_strikes(strikes)

    assert storm.strike_count == 2
    assert storm.is_dormant is False
    assert storm._dirty is True
    assert len(storm._strike_history) == 2


def test_is_expired_true_after_threshold(storm_module):
    storm = storm_module.Storm()
    storm.last_update = time.time() - 10 * 60  # 10 min geleden
    assert storm.is_expired(expire_minutes=5.0) is True
    assert storm.is_expired(expire_minutes=15.0) is False


def test_strikes_in_window_filters_by_age(storm_module):
    storm = storm_module.Storm()
    now = time.time()
    storm._strike_history = [
        (now - 400, 51.0, 4.0),   # ouder dan 5 min -> buiten venster
        (now - 60,  51.1, 4.1),   # binnen 5 min venster
    ]
    recent = storm.strikes_in_window(minutes=5)
    assert len(recent) == 1
    assert recent[0][1] == 51.1


def test_update_counts_reflects_recent_strikes(storm_module):
    storm = storm_module.Storm()
    now = time.time()
    storm._strike_history = [(now, 51.0, 4.0) for _ in range(3)]
    storm.update_counts()
    assert storm.strikes_5min == 3
    assert storm.strikes_60min == 3


def test_prune_history_removes_old_strikes(storm_module):
    storm = storm_module.Storm()
    now = time.time()
    storm._strike_history = [
        (now - 100 * 60, 51.0, 4.0),  # 100 min oud -> weg bij default max_age=90
        (now - 10 * 60,  51.1, 4.1),  # blijft
    ]
    storm.prune_history(max_age_minutes=90)
    assert len(storm._strike_history) == 1
    assert storm._strike_history[0][1] == 51.1
