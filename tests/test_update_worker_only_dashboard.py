from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_SCRIPT = Path(__file__).parents[1] / "scripts" / "update_worker_only_dashboard.py"
_SPEC = importlib.util.spec_from_file_location(
    "update_worker_only_dashboard_test_module", _SCRIPT
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def _split_dashboard(content: str) -> dict[str, object]:
    return {
        "views": [
            {
                "title": "Resultaten",
                "path": "resultaten",
                "sections": [{"type": "grid", "cards": [{"type": "map"}]}],
            },
            {
                "title": "Monitoring",
                "path": "monitoring",
                "sections": [
                    {
                        "type": "grid",
                        "cards": [
                            {
                                "type": "heading",
                                "heading": "Systeem en verwerking",
                            },
                            {"type": "markdown", "content": content},
                        ],
                    },
                    {
                        "type": "grid",
                        "cards": [
                            {
                                "type": "heading",
                                "heading": "Alle databronnen",
                            },
                            {"type": "entities", "title": "Radar"},
                        ],
                    },
                ],
            },
        ]
    }


def test_split_dashboard_refreshes_precipitation_card_from_worker_status() -> None:
    current = _split_dashboard("Status\n**Wachtrij:** 0\n")
    current["views"][0]["sections"] = [
        {
            "type": "grid",
            "cards": [
                {
                    "type": "heading",
                    "heading": "Neerslag en waarschuwingen",
                },
                {
                    "type": "markdown",
                    "content": "**actuele worker:** unknown",
                    "entity_id": ["sensor.stv3_worker_technical_diagnostics"],
                },
            ],
        }
    ]

    candidate = _MODULE._preserve_split_dashboard(current)

    card = candidate["views"][0]["sections"][0]["cards"][1]
    assert card["entity_id"] == [
        "sensor.storm_tracker_thuis_neerslag",
        "sensor.stv3_worker_status",
    ]
    assert "states(worker)" in card["content"]
    assert "state_attr(worker, 'worker_state')" not in card["content"]
    assert "state_attr(diagnostic, 'worker')" not in card["content"]


def test_split_dashboard_replaces_legacy_worker_ballast() -> None:
    current = _split_dashboard("Status\n**Wachtrij:** 0\n")

    candidate = _MODULE._preserve_split_dashboard(current)

    assert candidate is not None
    assert candidate["views"][0] == current["views"][0]
    content = candidate["views"][1]["sections"][0]["cards"][1]["content"]
    assert "**Celtracking:**" in content
    assert "**Fictieve tracker:**" in content
    assert "device_tracker.storm_tracker_fictieve_tracker" in content
    assert "latitude" in content
    assert "longitude" in content
    assert "**Ruwe kalibratie:**" not in content
    assert "**Snapshotopbouw:**" not in content
    assert "**Ophaling:**" not in content
    assert "**Wachtrij:**" not in content


def test_system_summary_replacement_is_idempotent() -> None:
    current = _split_dashboard("Status\n**Wachtrij:** 0\n")

    first = _MODULE._preserve_split_dashboard(current)
    second = _MODULE._preserve_split_dashboard(first)

    assert second == first


def test_split_dashboard_adds_motion_tracking_visibility() -> None:
    current = _split_dashboard("Status\n**Wachtrij:** 0\n")

    candidate = _MODULE._preserve_split_dashboard(current)

    content = candidate["views"][1]["sections"][0]["cards"][1]["content"]
    assert "**Celtracking:**" in content
    assert "tracking_active_tracks" in content
    assert "tracking_motion_ready_tracks" in content
    assert "tracking_shadow_arrival_candidates" in content


def test_split_dashboard_replaces_provider_rows_with_dual_status_cards() -> None:
    candidate = _MODULE._preserve_split_dashboard(
        _split_dashboard("Status\n**Wachtrij:** 0\n")
    )

    cards = candidate["views"][1]["sections"][1]["cards"]
    assert cards[0]["heading"] == "Alle databronnen"
    assert "Dubbele status" in cards[1]["content"]
    assert len(cards) == 6
    rendered = "\n".join(card.get("content", "") for card in cards)
    assert "readiness" in rendered
    assert "effective_status" in rendered
    assert "Ready" in rendered
    assert "Buiten bereik" in rendered
    assert "sensor.storm_tracker_bron_radar_kmi_appv4_visual" in rendered
    assert "sensor.storm_tracker_bron_radar_kmi_public_wcs_unknown_quantity" not in rendered


def test_provider_status_cards_use_aligned_three_column_tables() -> None:
    candidate = _MODULE._preserve_split_dashboard(
        _split_dashboard("Status\n**Wachtrij:** 0\n")
    )

    cards = candidate["views"][1]["sections"][1]["cards"]
    provider_tables = cards[2:]
    assert provider_tables
    for card in provider_tables:
        content = card["content"]
        assert "<table>" in content
        assert (
            "<th>Actuele status</th><th>Gereedheid</th><th>Bron</th>"
            in content
        )
        assert "<tr><td>" in content
        assert "replace('Storm Tracker bron ', '')" in content


def test_non_split_dashboard_keeps_legacy_candidate_path() -> None:
    current = {"views": [{"path": "storm-tracker", "sections": []}]}

    assert _MODULE._preserve_split_dashboard(current) is None


def test_split_dashboard_updates_legacy_provider_groups() -> None:
    current = {
        "views": [
            {"path": "resultaten", "sections": []},
            {
                "path": "monitoring",
                "sections": [
                    {
                        "cards": [
                            {"type": "heading", "heading": "Systeem en verwerking"},
                            {"type": "markdown", "content": "**Bronnen:** bestaand"},
                        ]
                    },
                    {
                        "cards": [
                            {"type": "heading", "heading": "Radar"},
                            {"type": "markdown", "content": "oud"},
                        ]
                    },
                    {
                        "cards": [
                            {
                                "type": "heading",
                                "heading": "Grond, modellen en validatie",
                            },
                            {"type": "markdown", "content": "oud"},
                            {"type": "heading", "heading": "Validatie"},
                            {"type": "markdown", "content": "oud"},
                        ]
                    },
                    {
                        "cards": [
                            {"type": "heading", "heading": "Bliksem en satelliet"},
                            {"type": "markdown", "content": "oud"},
                        ]
                    },
                ],
            },
        ]
    }

    candidate = _MODULE._preserve_split_dashboard(current)

    assert candidate is not None
    contents = [
        card["content"]
        for section in candidate["views"][1]["sections"]
        for card in section["cards"]
        if card.get("type") == "markdown"
    ]
    assert sum("Nog niet ready" in content for content in contents) == 4
    assert sum("effective_status" in content for content in contents) == 4
