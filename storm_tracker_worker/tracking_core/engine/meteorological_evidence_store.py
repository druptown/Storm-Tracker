"""Append-only vNext store for meteorological evidence and provenance.

This module is intentionally isolated from the live integration.  It stores
exact bytes and immutable derived records, but never URLs, request headers or
credentials.  Every tracker forecast has a foreign-key-backed provenance path
to an upstream raw receipt.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, is_dataclass
import hashlib
import json
import math
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


METEOROLOGICAL_EVIDENCE_SCHEMA_VERSION = 3
_STORE_KIND = "storm-tracker-meteorological-evidence"
_GROUND_DECISION_PROVENANCE_CONTRACT = (
    "stv3-ground-decision-provenance-v1"
)


class EvidenceStoreError(RuntimeError):
    """Base error for the isolated meteorological evidence store."""


class EvidenceSchemaError(EvidenceStoreError):
    """The database is corrupt, incomplete, or has an unknown schema."""


class EvidenceConflictError(EvidenceStoreError):
    """A logical identity was reused for different immutable content."""


class EvidenceForeignKeyError(EvidenceStoreError):
    """A record references evidence that has not been stored."""


_TABLE_DEFINITIONS = {
    "meteorological_evidence_metadata": """
        CREATE TABLE meteorological_evidence_metadata (
            metadata_key TEXT PRIMARY KEY,
            metadata_value TEXT NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_evidence_schema_migrations": """
        CREATE TABLE meteorological_evidence_schema_migrations (
            migration_id TEXT PRIMARY KEY,
            from_version INTEGER NOT NULL,
            to_version INTEGER NOT NULL,
            applied_at_ns INTEGER NOT NULL,
            CHECK(from_version >= 0),
            CHECK(to_version > from_version)
        ) WITHOUT ROWID
    """,
    "meteorological_evidence_conflict_events": """
        CREATE TABLE meteorological_evidence_conflict_events (
            conflict_id TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            logical_key TEXT NOT NULL,
            existing_record_id TEXT NOT NULL,
            attempted_record_id TEXT NOT NULL,
            attempted_record_json TEXT NOT NULL,
            detected_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_raw_blobs": """
        CREATE TABLE meteorological_raw_blobs (
            content_sha256 TEXT PRIMARY KEY CHECK(length(content_sha256)=64),
            payload BLOB NOT NULL,
            size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_raw_receipts": """
        CREATE TABLE meteorological_raw_receipts (
            receipt_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            provider_id TEXT NOT NULL,
            upstream_record_id TEXT NOT NULL,
            content_sha256 TEXT NOT NULL REFERENCES
                meteorological_raw_blobs(content_sha256),
            observed_start REAL,
            observed_end REAL,
            received_at REAL NOT NULL,
            time_quality_json TEXT NOT NULL,
            media_type TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_source_revision_events": """
        CREATE TABLE meteorological_source_revision_events (
            source_revision_event_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            provider_id TEXT NOT NULL,
            upstream_record_id TEXT NOT NULL,
            prior_receipt_id TEXT NOT NULL REFERENCES
                meteorological_raw_receipts(receipt_id),
            revised_receipt_id TEXT NOT NULL REFERENCES
                meteorological_raw_receipts(receipt_id),
            prior_content_sha256 TEXT NOT NULL REFERENCES
                meteorological_raw_blobs(content_sha256),
            revised_content_sha256 TEXT NOT NULL REFERENCES
                meteorological_raw_blobs(content_sha256),
            conflict_kind TEXT NOT NULL,
            detected_at_ns INTEGER NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(prior_receipt_id <> revised_receipt_id),
            CHECK(prior_content_sha256 <> revised_content_sha256)
        ) WITHOUT ROWID
    """,
    "meteorological_observations": """
        CREATE TABLE meteorological_observations (
            observation_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            receipt_id TEXT NOT NULL REFERENCES
                meteorological_raw_receipts(receipt_id),
            source_observation_id TEXT NOT NULL,
            observed_start REAL,
            observed_end REAL,
            received_at REAL NOT NULL,
            time_quality_json TEXT NOT NULL,
            quantity TEXT NOT NULL,
            unit TEXT NOT NULL,
            value_numeric REAL,
            categorical_json TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            native_projection TEXT NOT NULL,
            native_resolution_x REAL,
            native_resolution_y REAL,
            parser_version TEXT NOT NULL,
            calibration_id TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(latitude IS NULL OR (latitude >= -90 AND latitude <= 90)),
            CHECK(longitude IS NULL OR (longitude >= -180 AND longitude <= 180)),
            CHECK(native_resolution_x IS NULL OR native_resolution_x > 0),
            CHECK(native_resolution_y IS NULL OR native_resolution_y > 0)
        ) WITHOUT ROWID
    """,
    "meteorological_native_frames": """
        CREATE TABLE meteorological_native_frames (
            native_frame_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            receipt_id TEXT NOT NULL REFERENCES
                meteorological_raw_receipts(receipt_id),
            source_frame_id TEXT NOT NULL,
            observed_start REAL,
            observed_end REAL,
            received_at REAL NOT NULL,
            time_quality_json TEXT NOT NULL,
            quantity TEXT NOT NULL,
            unit TEXT NOT NULL,
            categorical_json TEXT NOT NULL,
            native_projection TEXT NOT NULL,
            width INTEGER NOT NULL CHECK(width > 0),
            height INTEGER NOT NULL CHECK(height > 0),
            native_resolution_x REAL NOT NULL CHECK(native_resolution_x > 0),
            native_resolution_y REAL NOT NULL CHECK(native_resolution_y > 0),
            encoding TEXT NOT NULL,
            frame_sha256 TEXT NOT NULL CHECK(length(frame_sha256)=64),
            frame_payload BLOB NOT NULL,
            parser_version TEXT NOT NULL,
            calibration_id TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_analysis_frames": """
        CREATE TABLE meteorological_analysis_frames (
            analysis_frame_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            native_frame_id TEXT NOT NULL REFERENCES
                meteorological_native_frames(native_frame_id),
            analysis_grid_id TEXT NOT NULL,
            observed_start REAL NOT NULL,
            observed_end REAL NOT NULL,
            quantity TEXT NOT NULL,
            unit TEXT NOT NULL,
            analysis_projection TEXT NOT NULL,
            width INTEGER NOT NULL CHECK(width > 0),
            height INTEGER NOT NULL CHECK(height > 0),
            resolution_x REAL NOT NULL CHECK(resolution_x > 0),
            resolution_y REAL NOT NULL CHECK(resolution_y > 0),
            encoding TEXT NOT NULL,
            frame_sha256 TEXT NOT NULL CHECK(length(frame_sha256)=64),
            frame_payload BLOB NOT NULL,
            transform_version TEXT NOT NULL,
            calibration_id TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(observed_end >= observed_start)
        ) WITHOUT ROWID
    """,
    "meteorological_detections": """
        CREATE TABLE meteorological_detections (
            detection_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            analysis_frame_id TEXT NOT NULL REFERENCES
                meteorological_analysis_frames(analysis_frame_id),
            detection_label TEXT NOT NULL,
            valid_at REAL NOT NULL,
            geometry_json TEXT NOT NULL,
            centroid_x REAL NOT NULL,
            centroid_y REAL NOT NULL,
            area_m2 REAL NOT NULL CHECK(area_m2 >= 0),
            maximum_value REAL,
            mean_value REAL,
            uncertainty_json TEXT NOT NULL,
            detector_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_flow_runs": """
        CREATE TABLE meteorological_flow_runs (
            flow_run_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            from_analysis_frame_id TEXT NOT NULL REFERENCES
                meteorological_analysis_frames(analysis_frame_id),
            to_analysis_frame_id TEXT NOT NULL REFERENCES
                meteorological_analysis_frames(analysis_frame_id),
            valid_start REAL NOT NULL,
            valid_end REAL NOT NULL,
            width INTEGER NOT NULL CHECK(width > 0),
            height INTEGER NOT NULL CHECK(height > 0),
            field_encoding TEXT NOT NULL,
            field_sha256 TEXT NOT NULL CHECK(length(field_sha256)=64),
            field_payload BLOB NOT NULL,
            method_version TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            quality_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(valid_end > valid_start),
            CHECK(from_analysis_frame_id <> to_analysis_frame_id)
        ) WITHOUT ROWID
    """,
    "meteorological_flow_samples": """
        CREATE TABLE meteorological_flow_samples (
            flow_sample_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            flow_run_id TEXT NOT NULL REFERENCES
                meteorological_flow_runs(flow_run_id),
            sample_label TEXT NOT NULL,
            x_m REAL NOT NULL,
            y_m REAL NOT NULL,
            displacement_x_m REAL NOT NULL,
            displacement_y_m REAL NOT NULL,
            velocity_x_mps REAL NOT NULL,
            velocity_y_mps REAL NOT NULL,
            covariance_json TEXT NOT NULL,
            quality_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_motion_estimates": """
        CREATE TABLE meteorological_motion_estimates (
            motion_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            from_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            to_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            flow_run_id TEXT REFERENCES
                meteorological_flow_runs(flow_run_id),
            flow_sample_id TEXT REFERENCES
                meteorological_flow_samples(flow_sample_id),
            valid_start REAL NOT NULL,
            valid_end REAL NOT NULL,
            displacement_x_m REAL NOT NULL,
            displacement_y_m REAL NOT NULL,
            velocity_x_mps REAL NOT NULL,
            velocity_y_mps REAL NOT NULL,
            covariance_json TEXT NOT NULL,
            evidence_detail_json TEXT NOT NULL,
            method_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(valid_end > valid_start),
            CHECK(from_detection_id <> to_detection_id)
        ) WITHOUT ROWID
    """,
    "meteorological_association_candidates": """
        CREATE TABLE meteorological_association_candidates (
            candidate_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            from_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            to_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            motion_id TEXT REFERENCES
                meteorological_motion_estimates(motion_id),
            cost REAL NOT NULL,
            feature_json TEXT NOT NULL,
            gate_json TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(from_detection_id <> to_detection_id)
        ) WITHOUT ROWID
    """,
    "meteorological_association_decisions": """
        CREATE TABLE meteorological_association_decisions (
            association_decision_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            candidate_id TEXT NOT NULL REFERENCES
                meteorological_association_candidates(candidate_id),
            assignment_solution_id TEXT REFERENCES
                meteorological_assignment_solutions(
                    assignment_solution_id
                ),
            decision TEXT NOT NULL,
            candidate_rank INTEGER NOT NULL CHECK(candidate_rank >= 0),
            rationale_json TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_assignment_runs": """
        CREATE TABLE meteorological_assignment_runs (
            assignment_run_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            region_id TEXT NOT NULL,
            from_analysis_frame_id TEXT NOT NULL REFERENCES
                meteorological_analysis_frames(analysis_frame_id),
            to_analysis_frame_id TEXT NOT NULL REFERENCES
                meteorological_analysis_frames(analysis_frame_id),
            valid_start REAL NOT NULL,
            valid_end REAL NOT NULL,
            candidate_count INTEGER NOT NULL CHECK(candidate_count >= 0),
            candidate_set_sha256 TEXT NOT NULL
                CHECK(length(candidate_set_sha256)=64),
            parameters_json TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(valid_end > valid_start),
            CHECK(from_analysis_frame_id <> to_analysis_frame_id)
        ) WITHOUT ROWID
    """,
    "meteorological_assignment_run_candidates": """
        CREATE TABLE meteorological_assignment_run_candidates (
            run_candidate_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            assignment_run_id TEXT NOT NULL REFERENCES
                meteorological_assignment_runs(assignment_run_id),
            candidate_id TEXT NOT NULL REFERENCES
                meteorological_association_candidates(candidate_id),
            candidate_rank INTEGER NOT NULL CHECK(candidate_rank >= 0),
            created_at_ns INTEGER NOT NULL,
            UNIQUE(assignment_run_id, candidate_id),
            UNIQUE(assignment_run_id, candidate_rank)
        ) WITHOUT ROWID
    """,
    "meteorological_assignment_solutions": """
        CREATE TABLE meteorological_assignment_solutions (
            assignment_solution_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            assignment_run_id TEXT NOT NULL REFERENCES
                meteorological_assignment_runs(assignment_run_id),
            solution_rank INTEGER NOT NULL CHECK(solution_rank >= 0),
            total_cost REAL NOT NULL,
            member_count INTEGER NOT NULL CHECK(member_count >= 0),
            member_set_sha256 TEXT NOT NULL CHECK(length(member_set_sha256)=64),
            set_rationale_json TEXT NOT NULL,
            solver_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            UNIQUE(assignment_run_id, solution_rank)
        ) WITHOUT ROWID
    """,
    "meteorological_assignment_solution_members": """
        CREATE TABLE meteorological_assignment_solution_members (
            assignment_member_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            assignment_solution_id TEXT NOT NULL REFERENCES
                meteorological_assignment_solutions(
                    assignment_solution_id
                ),
            member_role TEXT NOT NULL,
            candidate_id TEXT REFERENCES
                meteorological_association_candidates(candidate_id),
            detection_id TEXT REFERENCES
                meteorological_detections(detection_id),
            alternative_rank INTEGER CHECK(alternative_rank IS NULL
                OR alternative_rank >= 0),
            member_cost REAL,
            rationale_json TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            CHECK(member_role IN (
                'selected_match', 'birth', 'death', 'top_alternative'
            )),
            CHECK(
                (member_role IN ('selected_match', 'top_alternative')
                    AND candidate_id IS NOT NULL
                    AND detection_id IS NULL)
                OR
                (member_role IN ('birth', 'death')
                    AND candidate_id IS NULL
                    AND detection_id IS NOT NULL)
            ),
            UNIQUE(assignment_solution_id, ordinal)
        ) WITHOUT ROWID
    """,
    "meteorological_cell_tracks": """
        CREATE TABLE meteorological_cell_tracks (
            track_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            region_id TEXT NOT NULL,
            born_from_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_cell_track_snapshots": """
        CREATE TABLE meteorological_cell_track_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            detection_id TEXT REFERENCES
                meteorological_detections(detection_id),
            association_decision_id TEXT REFERENCES
                meteorological_association_decisions(
                    association_decision_id
                ),
            previous_snapshot_id TEXT REFERENCES
                meteorological_cell_track_snapshots(snapshot_id),
            valid_at REAL NOT NULL,
            state_json TEXT NOT NULL,
            covariance_json TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL,
            snapshot_kind TEXT NOT NULL,
            estimator_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(previous_snapshot_id IS NULL
                OR previous_snapshot_id <> snapshot_id),
            CHECK(snapshot_kind IN (
                'observed', 'associated', 'coasting', 'gap', 'predicted'
            )),
            CHECK(
                (detection_id IS NOT NULL
                    AND snapshot_kind='observed'
                    AND association_decision_id IS NULL)
                OR
                (detection_id IS NOT NULL
                    AND snapshot_kind='associated'
                    AND association_decision_id IS NOT NULL)
                OR
                (detection_id IS NULL
                    AND snapshot_kind IN ('coasting', 'gap', 'predicted')
                    AND association_decision_id IS NULL)
            )
        ) WITHOUT ROWID
    """,
    "meteorological_track_lineage_events": """
        CREATE TABLE meteorological_track_lineage_events (
            lineage_event_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            event_type TEXT NOT NULL,
            event_at REAL NOT NULL,
            member_count INTEGER NOT NULL CHECK(member_count >= 2),
            member_set_sha256 TEXT NOT NULL CHECK(length(member_set_sha256)=64),
            rationale_json TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_track_lineage_members": """
        CREATE TABLE meteorological_track_lineage_members (
            lineage_member_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            lineage_event_id TEXT NOT NULL REFERENCES
                meteorological_track_lineage_events(lineage_event_id),
            member_role TEXT NOT NULL,
            track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            snapshot_id TEXT NOT NULL REFERENCES
                meteorological_cell_track_snapshots(snapshot_id),
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            member_detail_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(member_role IN ('parent', 'child')),
            UNIQUE(lineage_event_id, ordinal),
            UNIQUE(lineage_event_id, member_role, track_id, snapshot_id)
        ) WITHOUT ROWID
    """,
    "meteorological_nowcast_runs": """
        CREATE TABLE meteorological_nowcast_runs (
            nowcast_run_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            source_snapshot_id TEXT NOT NULL REFERENCES
                meteorological_cell_track_snapshots(snapshot_id),
            analysis_frame_id TEXT NOT NULL REFERENCES
                meteorological_analysis_frames(analysis_frame_id),
            issued_at REAL NOT NULL,
            horizon_seconds REAL NOT NULL CHECK(horizon_seconds > 0),
            model_version TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_physical_sites": """
        CREATE TABLE meteorological_physical_sites (
            physical_site_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            latitude REAL NOT NULL CHECK(latitude >= -90 AND latitude <= 90),
            longitude REAL NOT NULL
                CHECK(longitude >= -180 AND longitude <= 180),
            elevation_m REAL,
            independence_group TEXT NOT NULL,
            site_metadata_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_physical_site_aliases": """
        CREATE TABLE meteorological_physical_site_aliases (
            site_alias_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            physical_site_id TEXT NOT NULL REFERENCES
                meteorological_physical_sites(physical_site_id),
            source_id TEXT NOT NULL,
            external_station_id TEXT NOT NULL,
            valid_start REAL,
            valid_end REAL,
            created_at_ns INTEGER NOT NULL,
            CHECK(valid_end IS NULL OR valid_start IS NULL
                OR valid_end > valid_start)
        ) WITHOUT ROWID
    """,
    "meteorological_ground_observations": """
        CREATE TABLE meteorological_ground_observations (
            ground_observation_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            receipt_id TEXT NOT NULL REFERENCES
                meteorological_raw_receipts(receipt_id),
            site_alias_id TEXT NOT NULL REFERENCES
                meteorological_physical_site_aliases(site_alias_id),
            source_observation_id TEXT NOT NULL,
            observed_start REAL,
            observed_end REAL,
            received_at REAL NOT NULL,
            time_quality_json TEXT NOT NULL,
            quantity TEXT NOT NULL,
            unit TEXT NOT NULL,
            value_numeric REAL,
            categorical_json TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            calibration_id TEXT NOT NULL,
            quality_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_ground_matches": """
        CREATE TABLE meteorological_ground_matches (
            ground_match_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            ground_observation_id TEXT NOT NULL REFERENCES
                meteorological_ground_observations(ground_observation_id),
            snapshot_id TEXT NOT NULL REFERENCES
                meteorological_cell_track_snapshots(snapshot_id),
            distance_m REAL NOT NULL CHECK(distance_m >= 0),
            temporal_overlap_seconds REAL NOT NULL
                CHECK(temporal_overlap_seconds >= 0),
            match_score REAL NOT NULL,
            match_detail_json TEXT NOT NULL,
            matcher_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_ground_decisions": """
        CREATE TABLE meteorological_ground_decisions (
            ground_decision_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            decision TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            reason_json TEXT NOT NULL,
            input_count INTEGER NOT NULL CHECK(input_count > 0),
            input_set_sha256 TEXT NOT NULL CHECK(length(input_set_sha256)=64),
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_ground_decision_inputs": """
        CREATE TABLE meteorological_ground_decision_inputs (
            ground_decision_input_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            ground_decision_id TEXT NOT NULL REFERENCES
                meteorological_ground_decisions(ground_decision_id),
            ground_match_id TEXT NOT NULL REFERENCES
                meteorological_ground_matches(ground_match_id),
            input_role TEXT NOT NULL,
            independence_group TEXT NOT NULL,
            input_reason_json TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            UNIQUE(ground_decision_id, ground_match_id, input_role),
            UNIQUE(ground_decision_id, ordinal)
        ) WITHOUT ROWID
    """,
    "meteorological_tracker_forecasts": """
        CREATE TABLE meteorological_tracker_forecasts (
            forecast_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            nowcast_run_id TEXT NOT NULL REFERENCES
                meteorological_nowcast_runs(nowcast_run_id),
            ground_decision_id TEXT REFERENCES
                meteorological_ground_decisions(ground_decision_id),
            tracker_id TEXT NOT NULL,
            issued_at REAL NOT NULL,
            valid_at REAL NOT NULL,
            predicted_latitude REAL NOT NULL
                CHECK(predicted_latitude >= -90
                    AND predicted_latitude <= 90),
            predicted_longitude REAL NOT NULL
                CHECK(predicted_longitude >= -180
                    AND predicted_longitude <= 180),
            precipitation_probability REAL NOT NULL
                CHECK(precipitation_probability >= 0
                    AND precipitation_probability <= 1),
            eta_seconds REAL,
            forecast_json TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            CHECK(valid_at >= issued_at)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_groupings": """
        CREATE TABLE meteorological_weather_system_groupings (
            weather_system_grouping_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            grouping_snapshot_id TEXT NOT NULL UNIQUE,
            valid_at REAL NOT NULL,
            analysis_grid_id TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            policy_fingerprint TEXT NOT NULL,
            policy_json TEXT NOT NULL,
            previous_grouping_snapshot_id TEXT,
            retired_system_ids_json TEXT NOT NULL,
            input_track_count INTEGER NOT NULL CHECK(input_track_count >= 0),
            system_count INTEGER NOT NULL CHECK(system_count >= 0),
            connection_count INTEGER NOT NULL CHECK(connection_count >= 0),
            membership_event_count INTEGER NOT NULL
                CHECK(membership_event_count >= 0),
            snapshot_json TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_track_inputs": """
        CREATE TABLE meteorological_weather_system_track_inputs (
            weather_system_track_input_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_grouping_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_groupings(
                    weather_system_grouping_id
                ),
            track_snapshot_id TEXT NOT NULL REFERENCES
                meteorological_cell_track_snapshots(snapshot_id),
            track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            current_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            analysis_grid_id TEXT NOT NULL,
            valid_at REAL NOT NULL,
            object_kind TEXT NOT NULL,
            current_footprint_json TEXT NOT NULL,
            rain_shield_detection_ids_json TEXT NOT NULL,
            x_m REAL NOT NULL,
            y_m REAL NOT NULL,
            vx_m_s REAL,
            vy_m_s REAL,
            motion_reference_valid_at REAL,
            motion_method TEXT,
            lifecycle_state TEXT NOT NULL,
            parent_track_ids_json TEXT NOT NULL,
            child_track_ids_json TEXT NOT NULL,
            lineage_event_ids_json TEXT NOT NULL,
            identity_alternative_track_ids_json TEXT NOT NULL,
            track_payload_json TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            CHECK(object_kind IN ('rain_shield', 'embedded_core')),
            CHECK(lifecycle_state IN ('tentative', 'confirmed', 'coasting')),
            CHECK(
                (vx_m_s IS NULL AND vy_m_s IS NULL
                    AND motion_reference_valid_at IS NULL
                    AND motion_method IS NULL)
                OR
                (vx_m_s IS NOT NULL AND vy_m_s IS NOT NULL
                    AND motion_reference_valid_at IS NOT NULL
                    AND motion_method IS NOT NULL)
            ),
            UNIQUE(weather_system_grouping_id, ordinal),
            UNIQUE(weather_system_grouping_id, track_id),
            UNIQUE(weather_system_grouping_id, track_snapshot_id)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_snapshots": """
        CREATE TABLE meteorological_weather_system_snapshots (
            weather_system_snapshot_record_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_grouping_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_groupings(
                    weather_system_grouping_id
                ),
            source_system_snapshot_id TEXT NOT NULL,
            system_id TEXT NOT NULL,
            valid_at REAL NOT NULL,
            analysis_grid_id TEXT NOT NULL,
            rain_shield_detection_ids_json TEXT NOT NULL,
            aggregate_footprint_json TEXT NOT NULL,
            aggregate_footprint_sha256 TEXT NOT NULL
                CHECK(length(aggregate_footprint_sha256)=64),
            descriptive_vx_m_s REAL,
            descriptive_vy_m_s REAL,
            descriptive_motion_method TEXT NOT NULL,
            motion_support_track_ids_json TEXT NOT NULL,
            maximum_member_residual_m_s REAL,
            classification TEXT NOT NULL CHECK(classification='not_classified'),
            system_uncertainty_json TEXT NOT NULL,
            uncertain_connection_ids_json TEXT NOT NULL,
            member_count INTEGER NOT NULL CHECK(member_count > 0),
            footprint_pixel_count INTEGER NOT NULL
                CHECK(footprint_pixel_count > 0),
            motion_support_count INTEGER NOT NULL
                CHECK(motion_support_count >= 0),
            predecessor_count INTEGER NOT NULL CHECK(predecessor_count >= 0),
            system_payload_json TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            CHECK(
                (descriptive_vx_m_s IS NULL
                    AND descriptive_vy_m_s IS NULL
                    AND maximum_member_residual_m_s IS NULL
                    AND motion_support_count=0)
                OR
                (descriptive_vx_m_s IS NOT NULL
                    AND descriptive_vy_m_s IS NOT NULL
                    AND maximum_member_residual_m_s IS NOT NULL
                    AND motion_support_count>0)
            ),
            UNIQUE(weather_system_grouping_id, source_system_snapshot_id),
            UNIQUE(weather_system_grouping_id, system_id),
            UNIQUE(weather_system_grouping_id, ordinal)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_members": """
        CREATE TABLE meteorological_weather_system_members (
            weather_system_member_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_snapshot_record_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_snapshots(
                    weather_system_snapshot_record_id
                ),
            weather_system_track_input_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_track_inputs(
                    weather_system_track_input_id
                ),
            track_snapshot_id TEXT NOT NULL REFERENCES
                meteorological_cell_track_snapshots(snapshot_id),
            track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            current_detection_id TEXT NOT NULL REFERENCES
                meteorological_detections(detection_id),
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            UNIQUE(weather_system_snapshot_record_id, ordinal),
            UNIQUE(weather_system_snapshot_record_id, track_id)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_footprint_pixels": """
        CREATE TABLE meteorological_weather_system_footprint_pixels (
            weather_system_footprint_pixel_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_snapshot_record_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_snapshots(
                    weather_system_snapshot_record_id
                ),
            pixel_row INTEGER NOT NULL CHECK(pixel_row >= 0),
            pixel_column INTEGER NOT NULL CHECK(pixel_column >= 0),
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            UNIQUE(weather_system_snapshot_record_id, ordinal),
            UNIQUE(
                weather_system_snapshot_record_id,
                pixel_row,
                pixel_column
            )
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_motion_members": """
        CREATE TABLE meteorological_weather_system_motion_members (
            weather_system_motion_member_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_snapshot_record_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_snapshots(
                    weather_system_snapshot_record_id
                ),
            weather_system_track_input_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_track_inputs(
                    weather_system_track_input_id
                ),
            track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            UNIQUE(weather_system_snapshot_record_id, ordinal),
            UNIQUE(weather_system_snapshot_record_id, track_id)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_connections": """
        CREATE TABLE meteorological_weather_system_connections (
            weather_system_connection_record_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_grouping_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_groupings(
                    weather_system_grouping_id
                ),
            source_connection_id TEXT NOT NULL,
            left_track_input_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_track_inputs(
                    weather_system_track_input_id
                ),
            right_track_input_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_track_inputs(
                    weather_system_track_input_id
                ),
            left_track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            right_track_id TEXT NOT NULL REFERENCES
                meteorological_cell_tracks(track_id),
            shared_rain_shield_detection_ids_json TEXT NOT NULL,
            overlap_pixel_count INTEGER NOT NULL
                CHECK(overlap_pixel_count >= 0),
            footprint_gap_m REAL NOT NULL CHECK(footprint_gap_m >= 0),
            valid_time_difference_s REAL NOT NULL
                CHECK(valid_time_difference_s >= 0),
            motion_time_difference_s REAL,
            motion_difference_m_s REAL,
            motion_coherence TEXT NOT NULL,
            spatial_relation TEXT NOT NULL,
            decision TEXT NOT NULL,
            reasons_json TEXT NOT NULL,
            connection_payload_json TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            CHECK(left_track_id < right_track_id),
            CHECK(motion_time_difference_s IS NULL
                OR motion_time_difference_s >= 0),
            CHECK(motion_difference_m_s IS NULL
                OR motion_difference_m_s >= 0),
            CHECK(motion_coherence IN (
                'coherent', 'incoherent', 'stale', 'unknown'
            )),
            CHECK(spatial_relation IN (
                'shared_rain_shield', 'overlap', 'touching', 'near', 'far'
            )),
            CHECK(decision IN ('connected', 'uncertain', 'separate')),
            UNIQUE(weather_system_grouping_id, source_connection_id),
            UNIQUE(weather_system_grouping_id, left_track_id, right_track_id),
            UNIQUE(weather_system_grouping_id, ordinal)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_predecessor_candidates": """
        CREATE TABLE meteorological_weather_system_predecessor_candidates (
            weather_system_predecessor_candidate_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_snapshot_record_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_snapshots(
                    weather_system_snapshot_record_id
                ),
            previous_system_id TEXT NOT NULL,
            direct_member_track_ids_json TEXT NOT NULL,
            lineage_supported_track_ids_json TEXT NOT NULL,
            selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
            predecessor_payload_json TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            UNIQUE(weather_system_snapshot_record_id, previous_system_id),
            UNIQUE(weather_system_snapshot_record_id, ordinal)
        ) WITHOUT ROWID
    """,
    "meteorological_weather_system_membership_events": """
        CREATE TABLE meteorological_weather_system_membership_events (
            weather_system_membership_event_record_id TEXT PRIMARY KEY,
            logical_key TEXT NOT NULL UNIQUE,
            record_hash TEXT NOT NULL CHECK(length(record_hash)=64),
            weather_system_grouping_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_groupings(
                    weather_system_grouping_id
                ),
            weather_system_snapshot_record_id TEXT NOT NULL REFERENCES
                meteorological_weather_system_snapshots(
                    weather_system_snapshot_record_id
                ),
            source_event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            current_system_id TEXT NOT NULL,
            selected_predecessor_system_id TEXT,
            predecessor_system_ids_json TEXT NOT NULL,
            alternative_predecessor_system_ids_json TEXT NOT NULL,
            current_member_track_ids_json TEXT NOT NULL,
            added_track_ids_json TEXT NOT NULL,
            removed_track_ids_json TEXT NOT NULL,
            member_lineage_event_ids_json TEXT NOT NULL,
            member_identity_alternative_track_ids_json TEXT NOT NULL,
            event_payload_json TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            created_at_ns INTEGER NOT NULL,
            CHECK(event_type IN (
                'birth', 'continued', 'membership_changed', 'merge',
                'split_retained', 'split_child', 'complex_reconfiguration'
            )),
            UNIQUE(weather_system_grouping_id, source_event_id),
            UNIQUE(weather_system_grouping_id, current_system_id),
            UNIQUE(weather_system_grouping_id, ordinal)
        ) WITHOUT ROWID
    """,
}

_WEATHER_SYSTEM_TABLES = tuple(
    table
    for table in _TABLE_DEFINITIONS
    if table.startswith("meteorological_weather_system_")
)
_IMMUTABLE_TABLES = tuple(_TABLE_DEFINITIONS)
_EXPECTED_TABLES = frozenset(_TABLE_DEFINITIONS)
_V2_IMMUTABLE_TABLES = tuple(
    table for table in _IMMUTABLE_TABLES if table not in _WEATHER_SYSTEM_TABLES
)
_V2_EXPECTED_TABLES = frozenset(_V2_IMMUTABLE_TABLES)
_RECORD_ID_COLUMNS = {
    "meteorological_raw_receipts": "receipt_id",
    "meteorological_source_revision_events": "source_revision_event_id",
    "meteorological_observations": "observation_id",
    "meteorological_native_frames": "native_frame_id",
    "meteorological_analysis_frames": "analysis_frame_id",
    "meteorological_detections": "detection_id",
    "meteorological_flow_runs": "flow_run_id",
    "meteorological_flow_samples": "flow_sample_id",
    "meteorological_motion_estimates": "motion_id",
    "meteorological_association_candidates": "candidate_id",
    "meteorological_association_decisions": "association_decision_id",
    "meteorological_assignment_runs": "assignment_run_id",
    "meteorological_assignment_run_candidates": "run_candidate_id",
    "meteorological_assignment_solutions": "assignment_solution_id",
    "meteorological_assignment_solution_members": "assignment_member_id",
    "meteorological_cell_tracks": "track_id",
    "meteorological_cell_track_snapshots": "snapshot_id",
    "meteorological_track_lineage_events": "lineage_event_id",
    "meteorological_track_lineage_members": "lineage_member_id",
    "meteorological_nowcast_runs": "nowcast_run_id",
    "meteorological_physical_sites": "physical_site_id",
    "meteorological_physical_site_aliases": "site_alias_id",
    "meteorological_ground_observations": "ground_observation_id",
    "meteorological_ground_matches": "ground_match_id",
    "meteorological_ground_decisions": "ground_decision_id",
    "meteorological_ground_decision_inputs": "ground_decision_input_id",
    "meteorological_tracker_forecasts": "forecast_id",
    "meteorological_weather_system_groupings": "weather_system_grouping_id",
    "meteorological_weather_system_track_inputs": (
        "weather_system_track_input_id"
    ),
    "meteorological_weather_system_snapshots": (
        "weather_system_snapshot_record_id"
    ),
    "meteorological_weather_system_members": "weather_system_member_id",
    "meteorological_weather_system_footprint_pixels": (
        "weather_system_footprint_pixel_id"
    ),
    "meteorological_weather_system_motion_members": (
        "weather_system_motion_member_id"
    ),
    "meteorological_weather_system_connections": (
        "weather_system_connection_record_id"
    ),
    "meteorological_weather_system_predecessor_candidates": (
        "weather_system_predecessor_candidate_id"
    ),
    "meteorological_weather_system_membership_events": (
        "weather_system_membership_event_record_id"
    ),
}

_SECRET_JSON_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "headers",
        "password",
        "secret",
        "token",
        "url",
        "uri",
    }
)


def stable_content_id(namespace: str, value: Mapping[str, Any]) -> str:
    """Return a deterministic, namespaced SHA-256 record identifier."""
    safe_namespace = _text(namespace, "namespace", 80)
    canonical = _canonical_json(value)
    digest = hashlib.sha256(
        safe_namespace.encode("utf-8") + b"\0" + canonical.encode("utf-8")
    ).hexdigest()
    return f"{safe_namespace}-{digest}"


class MeteorologicalEvidenceStore:
    """An isolated SQLite evidence ledger with no mutation operations."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._initialized = False
        self._lock = threading.RLock()

    def initialize(self) -> dict[str, int]:
        """Create v3, migrate an exact v2 ledger, or fail closed."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._lock, closing(
                self._connect(durable=False)
            ) as db, db:
                tables = self._table_names(db)
                if not tables:
                    self._configure_durability(db)
                    self._create_schema(db)
                    tables = self._table_names(db)
                elif tables == _V2_EXPECTED_TABLES:
                    self._migrate_v2_to_v3(db, tables)
                    tables = self._table_names(db)
                self._validate_schema(db, tables)
                self._configure_durability(db)
                result = self._stats(db)
        except sqlite3.DatabaseError as err:
            raise EvidenceSchemaError(
                "meteorological evidence database is niet leesbaar"
            ) from err
        self._initialized = True
        return result

    def append_raw_receipt(
        self,
        *,
        provider_id: str,
        upstream_record_id: str,
        payload: bytes,
        observed_start: float | None,
        observed_end: float | None,
        received_at: float,
        media_type: str,
        parser_version: str,
        time_quality: Mapping[str, Any] | None = None,
    ) -> str:
        """Atomically retain every exact upstream receipt and byte revision."""
        self._require_initialized()
        provider_id = _non_url_id(provider_id, "provider_id", 100)
        upstream_record_id = _non_url_id(
            upstream_record_id,
            "upstream_record_id",
            512,
        )
        media_type = _text(media_type, "media_type", 200)
        parser_version = _text(parser_version, "parser_version", 200)
        payload = _exact_bytes(payload, "payload")
        observed_start, observed_end, received_at, time_quality_json = (
            _evidence_interval(
            observed_start,
            observed_end,
            received_at,
                time_quality,
            )
        )
        content_sha256 = hashlib.sha256(payload).hexdigest()
        values = {
            "provider_id": provider_id,
            "upstream_record_id": upstream_record_id,
            "content_sha256": content_sha256,
            "observed_start": observed_start,
            "observed_end": observed_end,
            "received_at": received_at,
            "time_quality_json": time_quality_json,
            "media_type": media_type,
            "parser_version": parser_version,
        }
        logical_key = stable_content_id("raw-key", values)
        record_hash, record_id, attempted_json = self._identity(
            "raw-receipts",
            logical_key,
            values,
        )
        with self._lock, closing(self._verified_connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                blob = db.execute(
                    """
                    SELECT payload, size_bytes
                    FROM meteorological_raw_blobs
                    WHERE content_sha256=?
                    """,
                    (content_sha256,),
                ).fetchone()
                if blob is None:
                    db.execute(
                        """
                        INSERT INTO meteorological_raw_blobs(
                            content_sha256, payload, size_bytes, created_at_ns
                        ) VALUES(?, ?, ?, ?)
                        """,
                        (
                            content_sha256,
                            sqlite3.Binary(payload),
                            len(payload),
                            time.time_ns(),
                        ),
                    )
                elif (
                    bytes(blob["payload"]) != payload
                    or int(blob["size_bytes"]) != len(payload)
                ):
                    raise EvidenceConflictError(
                        "content hash verwijst naar andere exacte bytes"
                    )
                existing = self._existing(
                    db,
                    "meteorological_raw_receipts",
                    logical_key,
                )
                if existing is not None:
                    if str(existing["record_hash"]) == record_hash:
                        db.commit()
                        return str(existing["receipt_id"])
                    self._record_conflict(
                        db,
                        "meteorological_raw_receipts",
                        logical_key,
                        str(existing["receipt_id"]),
                        record_id,
                        attempted_json,
                    )
                    db.commit()
                    raise EvidenceConflictError(
                        "raw receipt-identiteit heeft andere immutable inhoud"
                    )
                prior = db.execute(
                    """
                    SELECT receipt_id, content_sha256
                    FROM meteorological_raw_receipts
                    WHERE provider_id=?
                      AND upstream_record_id=?
                    ORDER BY created_at_ns DESC, receipt_id DESC
                    LIMIT 1
                    """,
                    (provider_id, upstream_record_id),
                ).fetchone()
                columns = (
                    "receipt_id",
                    "logical_key",
                    "record_hash",
                    *values.keys(),
                    "created_at_ns",
                )
                db.execute(
                    self._insert_sql("meteorological_raw_receipts", columns),
                    (
                        record_id,
                        logical_key,
                        record_hash,
                        *values.values(),
                        time.time_ns(),
                    ),
                )
                if (
                    prior is not None
                    and str(prior["content_sha256"]) != content_sha256
                ):
                    revision_values = {
                        "provider_id": provider_id,
                        "upstream_record_id": upstream_record_id,
                        "prior_receipt_id": str(prior["receipt_id"]),
                        "revised_receipt_id": record_id,
                        "prior_content_sha256": str(
                            prior["content_sha256"]
                        ),
                        "revised_content_sha256": content_sha256,
                        "conflict_kind": "upstream_id_byte_revision",
                        "detected_at_ns": int(received_at * 1_000_000_000),
                    }
                    revision_logical = stable_content_id(
                        "source-revision-key",
                        {
                            "prior_receipt_id": revision_values[
                                "prior_receipt_id"
                            ],
                            "revised_receipt_id": record_id,
                        },
                    )
                    (
                        revision_hash,
                        revision_id,
                        _,
                    ) = self._identity(
                        "source-revision-events",
                        revision_logical,
                        revision_values,
                    )
                    revision_columns = (
                        "source_revision_event_id",
                        "logical_key",
                        "record_hash",
                        *revision_values.keys(),
                        "created_at_ns",
                    )
                    db.execute(
                        self._insert_sql(
                            "meteorological_source_revision_events",
                            revision_columns,
                        ),
                        (
                            revision_id,
                            revision_logical,
                            revision_hash,
                            *revision_values.values(),
                            time.time_ns(),
                        ),
                    )
                db.commit()
                return record_id
            except EvidenceConflictError:
                if db.in_transaction:
                    db.rollback()
                raise
            except sqlite3.IntegrityError as err:
                db.rollback()
                self._raise_integrity(err)
            except BaseException:
                db.rollback()
                raise

    store_raw_receipt = append_raw_receipt

    def append_observation(
        self,
        *,
        receipt_id: str,
        source_observation_id: str,
        observed_start: float | None,
        observed_end: float | None,
        received_at: float,
        quantity: str,
        unit: str,
        value_numeric: float | None = None,
        categorical: Mapping[str, Any] | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        native_projection: str,
        native_resolution_x: float | None = None,
        native_resolution_y: float | None = None,
        parser_version: str,
        calibration_id: str,
        time_quality: Mapping[str, Any] | None = None,
    ) -> str:
        interval = _evidence_interval(
            observed_start,
            observed_end,
            received_at,
            time_quality,
        )
        coordinates = _coordinates(latitude, longitude, allow_none=True)
        resolutions = _optional_resolution_pair(
            native_resolution_x,
            native_resolution_y,
        )
        categorical_json = _canonical_json(categorical or {})
        values = {
            "receipt_id": _record_id(receipt_id, "receipt_id"),
            "source_observation_id": _text(
                source_observation_id,
                "source_observation_id",
                512,
            ),
            "observed_start": interval[0],
            "observed_end": interval[1],
            "received_at": interval[2],
            "time_quality_json": interval[3],
            "quantity": _text(quantity, "quantity", 100),
            "unit": _text(unit, "unit", 80),
            "value_numeric": _optional_number(
                value_numeric,
                "value_numeric",
            ),
            "categorical_json": categorical_json,
            "latitude": coordinates[0],
            "longitude": coordinates[1],
            "native_projection": _text(
                native_projection,
                "native_projection",
                4000,
            ),
            "native_resolution_x": resolutions[0],
            "native_resolution_y": resolutions[1],
            "parser_version": _text(
                parser_version,
                "parser_version",
                200,
            ),
            "calibration_id": _text(
                calibration_id,
                "calibration_id",
                200,
            ),
        }
        logical = stable_content_id(
            "observation-key",
            {
                "receipt_id": values["receipt_id"],
                "source_observation_id": values["source_observation_id"],
            },
        )
        return self._append("meteorological_observations", logical, values)

    def append_native_frame(
        self,
        *,
        receipt_id: str,
        source_frame_id: str,
        observed_start: float | None,
        observed_end: float | None,
        received_at: float,
        quantity: str,
        unit: str,
        native_projection: str,
        width: int,
        height: int,
        native_resolution_x: float,
        native_resolution_y: float,
        encoding: str,
        frame_payload: bytes,
        parser_version: str,
        calibration_id: str,
        categorical: Mapping[str, Any] | None = None,
        time_quality: Mapping[str, Any] | None = None,
    ) -> str:
        interval = _evidence_interval(
            observed_start,
            observed_end,
            received_at,
            time_quality,
        )
        payload = _exact_bytes(frame_payload, "frame_payload")
        values = {
            "receipt_id": _record_id(receipt_id, "receipt_id"),
            "source_frame_id": _text(source_frame_id, "source_frame_id", 512),
            "observed_start": interval[0],
            "observed_end": interval[1],
            "received_at": interval[2],
            "time_quality_json": interval[3],
            "quantity": _text(quantity, "quantity", 100),
            "unit": _text(unit, "unit", 80),
            "categorical_json": _canonical_json(categorical or {}),
            "native_projection": _text(
                native_projection,
                "native_projection",
                4000,
            ),
            "width": _positive_int(width, "width"),
            "height": _positive_int(height, "height"),
            "native_resolution_x": _positive_number(
                native_resolution_x,
                "native_resolution_x",
            ),
            "native_resolution_y": _positive_number(
                native_resolution_y,
                "native_resolution_y",
            ),
            "encoding": _text(encoding, "encoding", 100),
            "frame_sha256": hashlib.sha256(payload).hexdigest(),
            "frame_payload": payload,
            "parser_version": _text(
                parser_version,
                "parser_version",
                200,
            ),
            "calibration_id": _text(
                calibration_id,
                "calibration_id",
                200,
            ),
        }
        logical = stable_content_id(
            "native-frame-key",
            {
                "receipt_id": values["receipt_id"],
                "source_frame_id": values["source_frame_id"],
            },
        )
        return self._append("meteorological_native_frames", logical, values)

    def append_analysis_frame(
        self,
        *,
        native_frame_id: str,
        analysis_grid_id: str,
        observed_start: float,
        observed_end: float,
        quantity: str,
        unit: str,
        analysis_projection: str,
        width: int,
        height: int,
        resolution_x: float,
        resolution_y: float,
        encoding: str,
        frame_payload: bytes,
        transform_version: str,
        calibration_id: str,
    ) -> str:
        start = _number(observed_start, "observed_start")
        end = _number(observed_end, "observed_end")
        if end < start:
            raise ValueError("observed_end moet na observed_start liggen")
        payload = _exact_bytes(frame_payload, "frame_payload")
        values = {
            "native_frame_id": _record_id(
                native_frame_id,
                "native_frame_id",
            ),
            "analysis_grid_id": _text(
                analysis_grid_id,
                "analysis_grid_id",
                200,
            ),
            "observed_start": start,
            "observed_end": end,
            "quantity": _text(quantity, "quantity", 100),
            "unit": _text(unit, "unit", 80),
            "analysis_projection": _text(
                analysis_projection,
                "analysis_projection",
                4000,
            ),
            "width": _positive_int(width, "width"),
            "height": _positive_int(height, "height"),
            "resolution_x": _positive_number(
                resolution_x,
                "resolution_x",
            ),
            "resolution_y": _positive_number(
                resolution_y,
                "resolution_y",
            ),
            "encoding": _text(encoding, "encoding", 100),
            "frame_sha256": hashlib.sha256(payload).hexdigest(),
            "frame_payload": payload,
            "transform_version": _text(
                transform_version,
                "transform_version",
                200,
            ),
            "calibration_id": _text(
                calibration_id,
                "calibration_id",
                200,
            ),
        }
        logical = stable_content_id(
            "analysis-frame-key",
            {
                "native_frame_id": values["native_frame_id"],
                "analysis_grid_id": values["analysis_grid_id"],
                "transform_version": values["transform_version"],
            },
        )
        return self._append("meteorological_analysis_frames", logical, values)

    def append_detection(
        self,
        *,
        analysis_frame_id: str,
        detection_label: str,
        valid_at: float,
        geometry: Mapping[str, Any],
        centroid_x: float,
        centroid_y: float,
        area_m2: float,
        detector_version: str,
        maximum_value: float | None = None,
        mean_value: float | None = None,
        uncertainty: Mapping[str, Any] | None = None,
    ) -> str:
        values = {
            "analysis_frame_id": _record_id(
                analysis_frame_id,
                "analysis_frame_id",
            ),
            "detection_label": _text(
                detection_label,
                "detection_label",
                200,
            ),
            "valid_at": _number(valid_at, "valid_at"),
            "geometry_json": _canonical_json(geometry),
            "centroid_x": _number(centroid_x, "centroid_x"),
            "centroid_y": _number(centroid_y, "centroid_y"),
            "area_m2": _nonnegative_number(area_m2, "area_m2"),
            "maximum_value": _optional_number(
                maximum_value,
                "maximum_value",
            ),
            "mean_value": _optional_number(mean_value, "mean_value"),
            "uncertainty_json": _canonical_json(uncertainty or {}),
            "detector_version": _text(
                detector_version,
                "detector_version",
                200,
            ),
        }
        logical = stable_content_id(
            "detection-key",
            {
                "analysis_frame_id": values["analysis_frame_id"],
                "detection_label": values["detection_label"],
            },
        )
        return self._append("meteorological_detections", logical, values)

    def append_flow_run(
        self,
        *,
        from_analysis_frame_id: str,
        to_analysis_frame_id: str,
        valid_start: float,
        valid_end: float,
        width: int,
        height: int,
        field_encoding: str,
        field_payload: bytes,
        method_version: str,
        parameters: Mapping[str, Any],
        quality: Mapping[str, Any],
    ) -> str:
        """Append one lossless motion-field run over two analysis frames."""
        start = _number(valid_start, "valid_start")
        end = _number(valid_end, "valid_end")
        if end <= start:
            raise ValueError("valid_end moet strikt na valid_start liggen")
        payload = _exact_bytes(field_payload, "field_payload")
        values = {
            "from_analysis_frame_id": _record_id(
                from_analysis_frame_id,
                "from_analysis_frame_id",
            ),
            "to_analysis_frame_id": _record_id(
                to_analysis_frame_id,
                "to_analysis_frame_id",
            ),
            "valid_start": start,
            "valid_end": end,
            "width": _positive_int(width, "width"),
            "height": _positive_int(height, "height"),
            "field_encoding": _text(
                field_encoding,
                "field_encoding",
                200,
            ),
            "field_sha256": hashlib.sha256(payload).hexdigest(),
            "field_payload": payload,
            "method_version": _text(
                method_version,
                "method_version",
                200,
            ),
            "parameters_json": _canonical_json(parameters),
            "quality_json": _canonical_json(quality),
        }
        logical = stable_content_id(
            "flow-run-key",
            {
                "from_analysis_frame_id": values[
                    "from_analysis_frame_id"
                ],
                "to_analysis_frame_id": values["to_analysis_frame_id"],
                "method_version": values["method_version"],
                "field_sha256": values["field_sha256"],
            },
        )
        return self._append("meteorological_flow_runs", logical, values)

    def append_flow_sample(
        self,
        *,
        flow_run_id: str,
        sample_label: str,
        x_m: float,
        y_m: float,
        displacement_x_m: float,
        displacement_y_m: float,
        velocity_x_mps: float,
        velocity_y_mps: float,
        covariance: Sequence[Sequence[float]],
        quality: Mapping[str, Any],
    ) -> str:
        """Append one immutable vector sampled from a stored flow field."""
        values = {
            "flow_run_id": _record_id(flow_run_id, "flow_run_id"),
            "sample_label": _text(
                sample_label,
                "sample_label",
                300,
            ),
            "x_m": _number(x_m, "x_m"),
            "y_m": _number(y_m, "y_m"),
            "displacement_x_m": _number(
                displacement_x_m,
                "displacement_x_m",
            ),
            "displacement_y_m": _number(
                displacement_y_m,
                "displacement_y_m",
            ),
            "velocity_x_mps": _number(
                velocity_x_mps,
                "velocity_x_mps",
            ),
            "velocity_y_mps": _number(
                velocity_y_mps,
                "velocity_y_mps",
            ),
            "covariance_json": _canonical_json(covariance),
            "quality_json": _canonical_json(quality),
        }
        logical = stable_content_id(
            "flow-sample-key",
            {
                "flow_run_id": values["flow_run_id"],
                "sample_label": values["sample_label"],
            },
        )
        return self._append("meteorological_flow_samples", logical, values)

    def append_motion_estimate(
        self,
        *,
        from_detection_id: str,
        to_detection_id: str,
        valid_start: float,
        valid_end: float,
        displacement_x_m: float,
        displacement_y_m: float,
        velocity_x_mps: float,
        velocity_y_mps: float,
        covariance: Sequence[Sequence[float]],
        method_version: str,
        flow_run_id: str | None = None,
        flow_sample_id: str | None = None,
        evidence_detail: Mapping[str, Any] | None = None,
    ) -> str:
        start = _number(valid_start, "valid_start")
        end = _number(valid_end, "valid_end")
        if end <= start:
            raise ValueError("valid_end moet strikt na valid_start liggen")
        source_detection = _record_id(
            from_detection_id,
            "from_detection_id",
        )
        target_detection = _record_id(
            to_detection_id,
            "to_detection_id",
        )
        run_id = (
            _record_id(flow_run_id, "flow_run_id")
            if flow_run_id is not None
            else None
        )
        sample_id = (
            _record_id(flow_sample_id, "flow_sample_id")
            if flow_sample_id is not None
            else None
        )
        if sample_id is not None and run_id is None:
            raise ValueError("flow_sample_id vereist flow_run_id")
        if run_id is not None:
            self._require_initialized()
            with self._lock, closing(self._verified_connection()) as db, db:
                relation = db.execute(
                    """
                    SELECT run.from_analysis_frame_id,
                           run.to_analysis_frame_id,
                           source.analysis_frame_id AS source_frame_id,
                           target.analysis_frame_id AS target_frame_id,
                           sample.flow_run_id AS sample_run_id
                    FROM meteorological_flow_runs AS run
                    JOIN meteorological_detections AS source
                        ON source.detection_id=?
                    JOIN meteorological_detections AS target
                        ON target.detection_id=?
                    LEFT JOIN meteorological_flow_samples AS sample
                        ON sample.flow_sample_id=?
                    WHERE run.flow_run_id=?
                    """,
                    (
                        source_detection,
                        target_detection,
                        sample_id,
                        run_id,
                    ),
                ).fetchone()
            if relation is None:
                raise EvidenceForeignKeyError(
                    "motion mist flow- of detectionprovenance"
                )
            if (
                str(relation["from_analysis_frame_id"])
                != str(relation["source_frame_id"])
                or str(relation["to_analysis_frame_id"])
                != str(relation["target_frame_id"])
                or (
                    sample_id is not None
                    and str(relation["sample_run_id"]) != run_id
                )
            ):
                raise EvidenceConflictError(
                    "motion verwijst naar een incompatibele flow-run/sample"
                )
        values = {
            "from_detection_id": source_detection,
            "to_detection_id": target_detection,
            "flow_run_id": run_id,
            "flow_sample_id": sample_id,
            "valid_start": start,
            "valid_end": end,
            "displacement_x_m": _number(
                displacement_x_m,
                "displacement_x_m",
            ),
            "displacement_y_m": _number(
                displacement_y_m,
                "displacement_y_m",
            ),
            "velocity_x_mps": _number(
                velocity_x_mps,
                "velocity_x_mps",
            ),
            "velocity_y_mps": _number(
                velocity_y_mps,
                "velocity_y_mps",
            ),
            "covariance_json": _canonical_json(covariance),
            "evidence_detail_json": _canonical_json(
                evidence_detail or {}
            ),
            "method_version": _text(
                method_version,
                "method_version",
                200,
            ),
        }
        logical = stable_content_id(
            "motion-key",
            {
                "from_detection_id": values["from_detection_id"],
                "to_detection_id": values["to_detection_id"],
                "flow_run_id": values["flow_run_id"],
                "flow_sample_id": values["flow_sample_id"],
                "method_version": values["method_version"],
            },
        )
        return self._append(
            "meteorological_motion_estimates",
            logical,
            values,
        )

    def append_association_candidate(
        self,
        *,
        from_detection_id: str,
        to_detection_id: str,
        cost: float,
        features: Mapping[str, Any],
        gates: Mapping[str, Any],
        algorithm_version: str,
        motion_id: str | None = None,
    ) -> str:
        values = {
            "from_detection_id": _record_id(
                from_detection_id,
                "from_detection_id",
            ),
            "to_detection_id": _record_id(
                to_detection_id,
                "to_detection_id",
            ),
            "motion_id": (
                _record_id(motion_id, "motion_id")
                if motion_id is not None
                else None
            ),
            "cost": _number(cost, "cost"),
            "feature_json": _canonical_json(features),
            "gate_json": _canonical_json(gates),
            "algorithm_version": _text(
                algorithm_version,
                "algorithm_version",
                200,
            ),
        }
        logical = stable_content_id(
            "association-candidate-key",
            {
                "from_detection_id": values["from_detection_id"],
                "to_detection_id": values["to_detection_id"],
                "algorithm_version": values["algorithm_version"],
            },
        )
        return self._append(
            "meteorological_association_candidates",
            logical,
            values,
        )

    def append_association_decision(
        self,
        *,
        candidate_id: str,
        decision: str,
        candidate_rank: int,
        rationale: Mapping[str, Any],
        algorithm_version: str,
        assignment_solution_id: str | None = None,
    ) -> str:
        values = {
            "candidate_id": _record_id(candidate_id, "candidate_id"),
            "assignment_solution_id": (
                _record_id(
                    assignment_solution_id,
                    "assignment_solution_id",
                )
                if assignment_solution_id is not None
                else None
            ),
            "decision": _text(decision, "decision", 80),
            "candidate_rank": _nonnegative_int(
                candidate_rank,
                "candidate_rank",
            ),
            "rationale_json": _canonical_json(rationale),
            "algorithm_version": _text(
                algorithm_version,
                "algorithm_version",
                200,
            ),
        }
        logical = stable_content_id(
            "association-decision-key",
            {
                "candidate_id": values["candidate_id"],
                "assignment_solution_id": values[
                    "assignment_solution_id"
                ],
                "algorithm_version": values["algorithm_version"],
            },
        )
        return self._append(
            "meteorological_association_decisions",
            logical,
            values,
        )

    def append_assignment_run(
        self,
        *,
        region_id: str,
        from_analysis_frame_id: str,
        to_analysis_frame_id: str,
        valid_start: float,
        valid_end: float,
        candidate_ids: Sequence[str],
        parameters: Mapping[str, Any],
        algorithm_version: str,
    ) -> str:
        """Append a complete candidate set for one global assignment run."""
        start = _number(valid_start, "valid_start")
        end = _number(valid_end, "valid_end")
        if end <= start:
            raise ValueError("valid_end moet strikt na valid_start liggen")
        candidates = tuple(
            _record_id(value, "candidate_id") for value in candidate_ids
        )
        if len(set(candidates)) != len(candidates):
            raise ValueError("candidate_ids mogen geen duplicaten bevatten")
        source_frame = _record_id(
            from_analysis_frame_id,
            "from_analysis_frame_id",
        )
        target_frame = _record_id(
            to_analysis_frame_id,
            "to_analysis_frame_id",
        )
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            frames_exist = db.execute(
                """
                SELECT
                    EXISTS(
                        SELECT 1 FROM meteorological_analysis_frames
                        WHERE analysis_frame_id=?
                    ),
                    EXISTS(
                        SELECT 1 FROM meteorological_analysis_frames
                        WHERE analysis_frame_id=?
                    )
                """,
                (source_frame, target_frame),
            ).fetchone()
            if tuple(frames_exist) != (1, 1):
                raise EvidenceForeignKeyError(
                    "assignment-run mist een analysis frame"
                )
            for candidate_id in candidates:
                relation = db.execute(
                    """
                    SELECT source.analysis_frame_id AS source_frame,
                           target.analysis_frame_id AS target_frame
                    FROM meteorological_association_candidates AS candidate
                    JOIN meteorological_detections AS source
                      ON source.detection_id=candidate.from_detection_id
                    JOIN meteorological_detections AS target
                      ON target.detection_id=candidate.to_detection_id
                    WHERE candidate.candidate_id=?
                    """,
                    (candidate_id,),
                ).fetchone()
                if relation is None:
                    raise EvidenceForeignKeyError(
                        "assignment-run mist een candidate"
                    )
                if (
                    str(relation["source_frame"]) != source_frame
                    or str(relation["target_frame"]) != target_frame
                ):
                    raise EvidenceConflictError(
                        "assignment candidate hoort bij andere frames"
                    )
        candidate_json = _canonical_json(list(candidates))
        values = {
            "region_id": _text(region_id, "region_id", 300),
            "from_analysis_frame_id": source_frame,
            "to_analysis_frame_id": target_frame,
            "valid_start": start,
            "valid_end": end,
            "candidate_count": len(candidates),
            "candidate_set_sha256": hashlib.sha256(
                candidate_json.encode("utf-8")
            ).hexdigest(),
            "parameters_json": _canonical_json(parameters),
            "algorithm_version": _text(
                algorithm_version,
                "algorithm_version",
                200,
            ),
        }
        logical = stable_content_id(
            "assignment-run-key",
            {
                "region_id": values["region_id"],
                "from_analysis_frame_id": values[
                    "from_analysis_frame_id"
                ],
                "to_analysis_frame_id": values["to_analysis_frame_id"],
                "valid_start": start,
                "valid_end": end,
                "algorithm_version": values["algorithm_version"],
                "candidate_set_sha256": values["candidate_set_sha256"],
            },
        )
        run_id = self._prospective_record_id(
            "meteorological_assignment_runs",
            logical,
            values,
        )
        records: list[tuple[str, str, Mapping[str, Any]]] = [
            ("meteorological_assignment_runs", logical, values)
        ]
        for rank, candidate_id in enumerate(candidates):
            member_values = {
                "assignment_run_id": run_id,
                "candidate_id": candidate_id,
                "candidate_rank": rank,
            }
            member_logical = stable_content_id(
                "assignment-run-candidate-key",
                {
                    "assignment_run_id": run_id,
                    "candidate_id": candidate_id,
                },
            )
            records.append(
                (
                    "meteorological_assignment_run_candidates",
                    member_logical,
                    member_values,
                )
            )
        self._append_records_atomically(records)
        return run_id

    def append_assignment_solution(
        self,
        *,
        assignment_run_id: str,
        solution_rank: int,
        total_cost: float,
        members: Sequence[Mapping[str, Any]],
        set_rationale: Mapping[str, Any],
        solver_version: str,
    ) -> str:
        """Append one ranked set-level solution and every member action."""
        run_id = _record_id(assignment_run_id, "assignment_run_id")
        rank = _nonnegative_int(solution_rank, "solution_rank")
        normalized: list[dict[str, Any]] = []
        allowed_roles = {
            "selected_match",
            "birth",
            "death",
            "top_alternative",
        }
        for ordinal, member in enumerate(members):
            if not isinstance(member, Mapping):
                raise ValueError("assignment member moet een mapping zijn")
            role = _text(member.get("role"), "member role", 80)
            if role not in allowed_roles:
                raise ValueError(f"onbekende assignment member-rol {role}")
            candidate_id = member.get("candidate_id")
            detection_id = member.get("detection_id")
            if role in {"selected_match", "top_alternative"}:
                if candidate_id is None or detection_id is not None:
                    raise ValueError(
                        f"{role} vereist alleen candidate_id"
                    )
            elif detection_id is None or candidate_id is not None:
                raise ValueError(f"{role} vereist alleen detection_id")
            alternative_rank = member.get("alternative_rank")
            if role == "top_alternative" and alternative_rank is None:
                raise ValueError(
                    "top_alternative vereist alternative_rank"
                )
            normalized.append(
                {
                    "member_role": role,
                    "candidate_id": (
                        _record_id(candidate_id, "candidate_id")
                        if candidate_id is not None
                        else None
                    ),
                    "detection_id": (
                        _record_id(detection_id, "detection_id")
                        if detection_id is not None
                        else None
                    ),
                    "alternative_rank": (
                        _nonnegative_int(
                            alternative_rank,
                            "alternative_rank",
                        )
                        if alternative_rank is not None
                        else None
                    ),
                    "member_cost": _optional_number(
                        member.get("cost"),
                        "member cost",
                    ),
                    "rationale_json": _canonical_json(
                        member.get("rationale") or {}
                    ),
                    "ordinal": ordinal,
                }
            )
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            run = db.execute(
                """
                SELECT from_analysis_frame_id, to_analysis_frame_id
                FROM meteorological_assignment_runs
                WHERE assignment_run_id=?
                """,
                (run_id,),
            ).fetchone()
            if run is None:
                raise EvidenceForeignKeyError(
                    "assignment solution mist assignment-run"
                )
            for item in normalized:
                if item["candidate_id"] is not None:
                    listed = db.execute(
                        """
                        SELECT 1
                        FROM meteorological_assignment_run_candidates
                        WHERE assignment_run_id=? AND candidate_id=?
                        """,
                        (run_id, item["candidate_id"]),
                    ).fetchone()
                    if listed is None:
                        raise EvidenceConflictError(
                            "solution-candidate zit niet in assignment-run"
                        )
                else:
                    detection = db.execute(
                        """
                        SELECT analysis_frame_id
                        FROM meteorological_detections
                        WHERE detection_id=?
                        """,
                        (item["detection_id"],),
                    ).fetchone()
                    if detection is None:
                        raise EvidenceForeignKeyError(
                            "assignment member mist detectie"
                        )
                    expected = (
                        str(run["to_analysis_frame_id"])
                        if item["member_role"] == "birth"
                        else str(run["from_analysis_frame_id"])
                    )
                    if str(detection["analysis_frame_id"]) != expected:
                        raise EvidenceConflictError(
                            "birth/death-detectie hoort bij verkeerd frame"
                        )
        member_json = _canonical_json(normalized)
        values = {
            "assignment_run_id": run_id,
            "solution_rank": rank,
            "total_cost": _number(total_cost, "total_cost"),
            "member_count": len(normalized),
            "member_set_sha256": hashlib.sha256(
                member_json.encode("utf-8")
            ).hexdigest(),
            "set_rationale_json": _canonical_json(set_rationale),
            "solver_version": _text(
                solver_version,
                "solver_version",
                200,
            ),
        }
        logical = stable_content_id(
            "assignment-solution-key",
            {
                "assignment_run_id": run_id,
                "solution_rank": rank,
            },
        )
        solution_id = self._prospective_record_id(
            "meteorological_assignment_solutions",
            logical,
            values,
        )
        records: list[tuple[str, str, Mapping[str, Any]]] = [
            ("meteorological_assignment_solutions", logical, values)
        ]
        for item in normalized:
            member_logical = stable_content_id(
                "assignment-solution-member-key",
                {
                    "assignment_solution_id": solution_id,
                    "ordinal": item["ordinal"],
                },
            )
            records.append(
                (
                    "meteorological_assignment_solution_members",
                    member_logical,
                    {
                        "assignment_solution_id": solution_id,
                        **item,
                    },
                )
            )
        self._append_records_atomically(records)
        return solution_id

    def append_cell_track(
        self,
        *,
        track_key: str,
        region_id: str,
        born_from_detection_id: str,
    ) -> str:
        logical = stable_content_id(
            "cell-track-key",
            {"track_key": _text(track_key, "track_key", 300)},
        )
        values = {
            "region_id": _text(region_id, "region_id", 300),
            "born_from_detection_id": _record_id(
                born_from_detection_id,
                "born_from_detection_id",
            ),
        }
        return self._append("meteorological_cell_tracks", logical, values)

    def append_track_snapshot(
        self,
        *,
        track_id: str,
        detection_id: str | None,
        valid_at: float,
        state: Mapping[str, Any],
        covariance: Sequence[Sequence[float]],
        lifecycle_state: str,
        estimator_version: str,
        association_decision_id: str | None = None,
        previous_snapshot_id: str | None = None,
        snapshot_kind: str | None = None,
    ) -> str:
        detection = (
            _record_id(detection_id, "detection_id")
            if detection_id is not None
            else None
        )
        if snapshot_kind is None:
            if detection is None:
                kind = "coasting"
            elif association_decision_id is not None:
                kind = "associated"
            else:
                kind = "observed"
        else:
            kind = _text(snapshot_kind, "snapshot_kind", 80)
        observed_kinds = {"observed", "associated"}
        predicted_kinds = {"coasting", "gap", "predicted"}
        if (
            detection is not None
            and kind not in observed_kinds
        ) or (
            detection is None
            and kind not in predicted_kinds
        ):
            raise ValueError(
                "snapshot_kind past niet bij aanwezigheid van detection_id"
            )
        if (
            kind == "associated"
            and association_decision_id is None
        ) or (
            kind != "associated"
            and association_decision_id is not None
        ):
            raise ValueError(
                "alleen associated snapshots vereisen een association decision"
            )
        values = {
            "track_id": _record_id(track_id, "track_id"),
            "detection_id": detection,
            "association_decision_id": (
                _record_id(
                    association_decision_id,
                    "association_decision_id",
                )
                if association_decision_id is not None
                else None
            ),
            "previous_snapshot_id": (
                _record_id(previous_snapshot_id, "previous_snapshot_id")
                if previous_snapshot_id is not None
                else None
            ),
            "valid_at": _number(valid_at, "valid_at"),
            "state_json": _canonical_json(state),
            "covariance_json": _canonical_json(covariance),
            "lifecycle_state": _text(
                lifecycle_state,
                "lifecycle_state",
                80,
            ),
            "snapshot_kind": kind,
            "estimator_version": _text(
                estimator_version,
                "estimator_version",
                200,
            ),
        }
        logical = stable_content_id(
            "track-snapshot-key",
            {
                "track_id": values["track_id"],
                "detection_id": values["detection_id"],
                "valid_at": values["valid_at"],
                "snapshot_kind": values["snapshot_kind"],
            },
        )
        return self._append(
            "meteorological_cell_track_snapshots",
            logical,
            values,
        )

    def append_track_lineage_event(
        self,
        *,
        event_key: str,
        event_type: str,
        event_at: float,
        members: Sequence[Mapping[str, Any]],
        algorithm_version: str,
        rationale: Mapping[str, Any] | None = None,
    ) -> str:
        """Append one grouped split/merge event with all parent/child legs."""
        normalized: list[dict[str, Any]] = []
        roles: set[str] = set()
        unique_legs: set[tuple[str, str, str]] = set()
        for ordinal, member in enumerate(members):
            if not isinstance(member, Mapping):
                raise ValueError("lineage member moet een mapping zijn")
            role = _text(member.get("role"), "member role", 20)
            if role not in {"parent", "child"}:
                raise ValueError("lineage member role moet parent/child zijn")
            track_id = _record_id(member.get("track_id"), "track_id")
            snapshot_id = _record_id(
                member.get("snapshot_id"),
                "snapshot_id",
            )
            leg = (role, track_id, snapshot_id)
            if leg in unique_legs:
                raise ValueError("lineage event bevat een dubbele member")
            unique_legs.add(leg)
            roles.add(role)
            normalized.append(
                {
                    "member_role": role,
                    "track_id": track_id,
                    "snapshot_id": snapshot_id,
                    "ordinal": ordinal,
                    "member_detail_json": _canonical_json(
                        member.get("detail") or {}
                    ),
                }
            )
        if roles != {"parent", "child"}:
            raise ValueError(
                "lineage event vereist minstens één parent en één child"
            )
        parent_tracks = {
            item["track_id"]
            for item in normalized
            if item["member_role"] == "parent"
        }
        child_tracks = {
            item["track_id"]
            for item in normalized
            if item["member_role"] == "child"
        }
        if parent_tracks & child_tracks:
            raise ValueError(
                "lineage parent- en childtracks moeten verschillend zijn"
            )
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            for item in normalized:
                snapshot = db.execute(
                    """
                    SELECT track_id
                    FROM meteorological_cell_track_snapshots
                    WHERE snapshot_id=?
                    """,
                    (item["snapshot_id"],),
                ).fetchone()
                if snapshot is None:
                    raise EvidenceForeignKeyError(
                        "lineage member mist snapshot"
                    )
                if str(snapshot["track_id"]) != item["track_id"]:
                    raise EvidenceConflictError(
                        "lineage snapshot hoort niet bij opgegeven track"
                    )
        member_json = _canonical_json(normalized)
        values = {
            "event_type": _text(event_type, "event_type", 80),
            "event_at": _number(event_at, "event_at"),
            "member_count": len(normalized),
            "member_set_sha256": hashlib.sha256(
                member_json.encode("utf-8")
            ).hexdigest(),
            "rationale_json": _canonical_json(rationale or {}),
            "algorithm_version": _text(
                algorithm_version,
                "algorithm_version",
                200,
            ),
        }
        logical = stable_content_id(
            "track-lineage-event-key",
            {
                "event_key": _text(event_key, "event_key", 300),
            },
        )
        event_id = self._prospective_record_id(
            "meteorological_track_lineage_events",
            logical,
            values,
        )
        records: list[tuple[str, str, Mapping[str, Any]]] = [
            ("meteorological_track_lineage_events", logical, values)
        ]
        for item in normalized:
            member_logical = stable_content_id(
                "track-lineage-member-key",
                {
                    "lineage_event_id": event_id,
                    "ordinal": item["ordinal"],
                },
            )
            records.append(
                (
                    "meteorological_track_lineage_members",
                    member_logical,
                    {
                        "lineage_event_id": event_id,
                        **item,
                    },
                )
            )
        self._append_records_atomically(records)
        return event_id

    def append_track_lineage(
        self,
        *,
        event_type: str,
        parent_track_id: str,
        child_track_id: str,
        parent_snapshot_id: str,
        child_snapshot_id: str,
        event_at: float,
        algorithm_version: str,
    ) -> str:
        """Compatibility wrapper for a one-parent/one-child lineage event."""
        return self.append_track_lineage_event(
            event_key=stable_content_id(
                "legacy-lineage-key",
                {
                    "event_type": event_type,
                    "parent_track_id": parent_track_id,
                    "child_track_id": child_track_id,
                    "event_at": event_at,
                },
            ),
            event_type=event_type,
            event_at=event_at,
            members=(
                {
                    "role": "parent",
                    "track_id": parent_track_id,
                    "snapshot_id": parent_snapshot_id,
                },
                {
                    "role": "child",
                    "track_id": child_track_id,
                    "snapshot_id": child_snapshot_id,
                },
            ),
            algorithm_version=algorithm_version,
        )

    def append_nowcast_run(
        self,
        *,
        source_snapshot_id: str,
        analysis_frame_id: str,
        issued_at: float,
        horizon_seconds: float,
        model_version: str,
        parameters: Mapping[str, Any],
    ) -> str:
        values = {
            "source_snapshot_id": _record_id(
                source_snapshot_id,
                "source_snapshot_id",
            ),
            "analysis_frame_id": _record_id(
                analysis_frame_id,
                "analysis_frame_id",
            ),
            "issued_at": _number(issued_at, "issued_at"),
            "horizon_seconds": _positive_number(
                horizon_seconds,
                "horizon_seconds",
            ),
            "model_version": _text(
                model_version,
                "model_version",
                200,
            ),
            "parameters_json": _canonical_json(parameters),
        }
        logical = stable_content_id(
            "nowcast-run-key",
            {
                "source_snapshot_id": values["source_snapshot_id"],
                "issued_at": values["issued_at"],
                "model_version": values["model_version"],
            },
        )
        return self._append("meteorological_nowcast_runs", logical, values)

    def append_physical_site(
        self,
        *,
        site_key: str,
        latitude: float,
        longitude: float,
        independence_group: str,
        elevation_m: float | None = None,
        site_metadata: Mapping[str, Any] | None = None,
    ) -> str:
        coordinates = _coordinates(latitude, longitude, allow_none=False)
        logical = stable_content_id(
            "physical-site-key",
            {"site_key": _text(site_key, "site_key", 300)},
        )
        values = {
            "latitude": coordinates[0],
            "longitude": coordinates[1],
            "elevation_m": _optional_number(elevation_m, "elevation_m"),
            "independence_group": _text(
                independence_group,
                "independence_group",
                300,
            ),
            "site_metadata_json": _canonical_json(site_metadata or {}),
        }
        return self._append("meteorological_physical_sites", logical, values)

    def append_site_alias(
        self,
        *,
        physical_site_id: str,
        source_id: str,
        external_station_id: str,
        valid_start: float | None = None,
        valid_end: float | None = None,
    ) -> str:
        start = _optional_number(valid_start, "valid_start")
        end = _optional_number(valid_end, "valid_end")
        if start is not None and end is not None and end <= start:
            raise ValueError("valid_end moet strikt na valid_start liggen")
        values = {
            "physical_site_id": _record_id(
                physical_site_id,
                "physical_site_id",
            ),
            "source_id": _non_url_id(source_id, "source_id", 100),
            "external_station_id": _non_url_id(
                external_station_id,
                "external_station_id",
                300,
            ),
            "valid_start": start,
            "valid_end": end,
        }
        logical = stable_content_id(
            "site-alias-key",
            {
                "source_id": values["source_id"],
                "external_station_id": values["external_station_id"],
                "valid_start": start,
                "valid_end": end,
            },
        )
        (
            record_hash,
            record_id,
            attempted_json,
        ) = self._identity("physical-site-aliases", logical, values)
        with self._lock, closing(self._verified_connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = self._existing(
                    db,
                    "meteorological_physical_site_aliases",
                    logical,
                )
                if existing is not None:
                    if str(existing["record_hash"]) == record_hash:
                        db.commit()
                        return str(existing["site_alias_id"])
                    self._record_conflict(
                        db,
                        "meteorological_physical_site_aliases",
                        logical,
                        str(existing["site_alias_id"]),
                        record_id,
                        attempted_json,
                    )
                    db.commit()
                    raise EvidenceConflictError(
                        "site-aliasinterval heeft andere immutable inhoud"
                    )
                overlap = db.execute(
                    """
                    SELECT site_alias_id
                    FROM meteorological_physical_site_aliases
                    WHERE source_id=?
                      AND external_station_id=?
                      AND (? IS NULL OR valid_end IS NULL
                           OR valid_end>?)
                      AND (? IS NULL OR valid_start IS NULL
                           OR valid_start<?)
                    ORDER BY created_at_ns, site_alias_id
                    LIMIT 1
                    """,
                    (
                        values["source_id"],
                        values["external_station_id"],
                        start,
                        start,
                        end,
                        end,
                    ),
                ).fetchone()
                if overlap is not None:
                    self._record_conflict(
                        db,
                        "meteorological_physical_site_aliases",
                        logical,
                        str(overlap["site_alias_id"]),
                        record_id,
                        attempted_json,
                    )
                    db.commit()
                    raise EvidenceConflictError(
                        "site-aliasinterval overlapt bestaande historie"
                    )
                columns = (
                    "site_alias_id",
                    "logical_key",
                    "record_hash",
                    *values.keys(),
                    "created_at_ns",
                )
                db.execute(
                    self._insert_sql(
                        "meteorological_physical_site_aliases",
                        columns,
                    ),
                    (
                        record_id,
                        logical,
                        record_hash,
                        *values.values(),
                        time.time_ns(),
                    ),
                )
                db.commit()
                return record_id
            except EvidenceConflictError:
                if db.in_transaction:
                    db.rollback()
                raise
            except sqlite3.IntegrityError as err:
                db.rollback()
                self._raise_integrity(err)
            except BaseException:
                db.rollback()
                raise

    def append_ground_observation(
        self,
        *,
        receipt_id: str,
        site_alias_id: str,
        source_observation_id: str,
        observed_start: float | None,
        observed_end: float | None,
        received_at: float,
        quantity: str,
        unit: str,
        parser_version: str,
        calibration_id: str,
        value_numeric: float | None = None,
        categorical: Mapping[str, Any] | None = None,
        quality: Mapping[str, Any] | None = None,
        time_quality: Mapping[str, Any] | None = None,
    ) -> str:
        interval = _evidence_interval(
            observed_start,
            observed_end,
            received_at,
            time_quality,
        )
        values = {
            "receipt_id": _record_id(receipt_id, "receipt_id"),
            "site_alias_id": _record_id(site_alias_id, "site_alias_id"),
            "source_observation_id": _text(
                source_observation_id,
                "source_observation_id",
                512,
            ),
            "observed_start": interval[0],
            "observed_end": interval[1],
            "received_at": interval[2],
            "time_quality_json": interval[3],
            "quantity": _text(quantity, "quantity", 100),
            "unit": _text(unit, "unit", 80),
            "value_numeric": _optional_number(
                value_numeric,
                "value_numeric",
            ),
            "categorical_json": _canonical_json(categorical or {}),
            "parser_version": _text(
                parser_version,
                "parser_version",
                200,
            ),
            "calibration_id": _text(
                calibration_id,
                "calibration_id",
                200,
            ),
            "quality_json": _canonical_json(quality or {}),
        }
        logical = stable_content_id(
            "ground-observation-key",
            {
                "receipt_id": values["receipt_id"],
                "site_alias_id": values["site_alias_id"],
                "source_observation_id": values["source_observation_id"],
            },
        )
        return self._append(
            "meteorological_ground_observations",
            logical,
            values,
        )

    def append_ground_match(
        self,
        *,
        ground_observation_id: str,
        snapshot_id: str,
        distance_m: float,
        temporal_overlap_seconds: float,
        match_score: float,
        match_details: Mapping[str, Any],
        matcher_version: str,
    ) -> str:
        values = {
            "ground_observation_id": _record_id(
                ground_observation_id,
                "ground_observation_id",
            ),
            "snapshot_id": _record_id(snapshot_id, "snapshot_id"),
            "distance_m": _nonnegative_number(distance_m, "distance_m"),
            "temporal_overlap_seconds": _nonnegative_number(
                temporal_overlap_seconds,
                "temporal_overlap_seconds",
            ),
            "match_score": _number(match_score, "match_score"),
            "match_detail_json": _canonical_json(match_details),
            "matcher_version": _text(
                matcher_version,
                "matcher_version",
                200,
            ),
        }
        logical = stable_content_id(
            "ground-match-key",
            {
                "ground_observation_id": values["ground_observation_id"],
                "snapshot_id": values["snapshot_id"],
                "matcher_version": values["matcher_version"],
            },
        )
        return self._append("meteorological_ground_matches", logical, values)

    def append_ground_decision(
        self,
        *,
        decision: str,
        policy_version: str,
        registry_fingerprint: str,
        reason: Mapping[str, Any],
        catalog_fingerprint: str | None = None,
        mapping_fingerprint: str | None = None,
        inputs: Sequence[Mapping[str, Any]] | None = None,
        ground_match_id: str | None = None,
        decision_key: str | None = None,
    ) -> str:
        """Append one fusion decision plus every independently weighted input."""
        registry_digest = _sha256_hex(
            registry_fingerprint,
            "registry_fingerprint",
        )
        catalog_digest = _optional_sha256_hex(
            catalog_fingerprint,
            "catalog_fingerprint",
        )
        if inputs is None:
            if ground_match_id is None:
                raise ValueError(
                    "ground decision vereist minstens één ground match"
                )
            if mapping_fingerprint is None:
                raise ValueError(
                    "ground decision vereist een mapping_fingerprint "
                    "voor iedere input"
                )
            input_rows: Sequence[Mapping[str, Any]] = (
                {
                    "ground_match_id": ground_match_id,
                    "role": "primary",
                    "reason": {},
                    "mapping_fingerprint": mapping_fingerprint,
                },
            )
        else:
            if ground_match_id is not None:
                raise ValueError(
                    "gebruik inputs of ground_match_id, niet beide"
                )
            if mapping_fingerprint is not None:
                raise ValueError(
                    "mapping_fingerprint hoort per item in inputs"
                )
            input_rows = inputs
        normalized: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        with self._lock, closing(self._verified_connection()) as db, db:
            for ordinal, item in enumerate(input_rows):
                if not isinstance(item, Mapping):
                    raise ValueError(
                        "ground decision input moet een mapping zijn"
                    )
                match_id = _record_id(
                    item.get("ground_match_id"),
                    "ground_match_id",
                )
                role = _text(
                    item.get("role", "supporting"),
                    "input_role",
                    80,
                )
                key = (match_id, role)
                if key in seen:
                    raise ValueError(
                        "ground decision bevat een dubbele match/rol"
                    )
                seen.add(key)
                site = db.execute(
                    """
                    SELECT site.independence_group
                    FROM meteorological_ground_matches AS match
                    JOIN meteorological_ground_observations AS observation
                        USING(ground_observation_id)
                    JOIN meteorological_physical_site_aliases AS alias
                        USING(site_alias_id)
                    JOIN meteorological_physical_sites AS site
                        USING(physical_site_id)
                    WHERE match.ground_match_id=?
                    """,
                    (match_id,),
                ).fetchone()
                if site is None:
                    raise EvidenceForeignKeyError(
                        "ground decision-input mist volledige siteprovenance"
                    )
                derived_group = str(site["independence_group"])
                supplied_group = item.get("independence_group")
                if (
                    supplied_group is not None
                    and _text(
                        supplied_group,
                        "independence_group",
                        300,
                    )
                    != derived_group
                ):
                    raise EvidenceConflictError(
                        "ground input independence_group wijkt af van site"
                    )
                normalized.append(
                    {
                        "ground_match_id": match_id,
                        "input_role": role,
                        "independence_group": derived_group,
                        "input_reason_json": _canonical_json(
                            {
                                "provenance_contract": (
                                    _GROUND_DECISION_PROVENANCE_CONTRACT
                                ),
                                "mapping_fingerprint": _sha256_hex(
                                    item.get("mapping_fingerprint"),
                                    "mapping_fingerprint",
                                ),
                                "reason": item.get("reason") or {},
                            }
                        ),
                        "ordinal": ordinal,
                    }
                )
        if not normalized:
            raise ValueError(
                "ground decision vereist minstens één ground match"
            )
        input_json = _canonical_json(normalized)
        values = {
            "decision": _text(decision, "decision", 80),
            "policy_version": _text(
                policy_version,
                "policy_version",
                200,
            ),
            "reason_json": _canonical_json(
                {
                    "provenance_contract": (
                        _GROUND_DECISION_PROVENANCE_CONTRACT
                    ),
                    "registry_fingerprint": registry_digest,
                    "catalog_fingerprint": catalog_digest,
                    "reason": reason,
                }
            ),
            "input_count": len(normalized),
            "input_set_sha256": hashlib.sha256(
                input_json.encode("utf-8")
            ).hexdigest(),
        }
        logical = stable_content_id(
            "ground-decision-key",
            {
                "decision_key": (
                    _text(decision_key, "decision_key", 300)
                    if decision_key is not None
                    else None
                ),
                "decision": values["decision"],
                "policy_version": values["policy_version"],
                "registry_fingerprint": registry_digest,
                "catalog_fingerprint": catalog_digest,
                "input_set_sha256": values["input_set_sha256"],
            },
        )
        decision_id = self._prospective_record_id(
            "meteorological_ground_decisions",
            logical,
            values,
        )
        records: list[tuple[str, str, Mapping[str, Any]]] = [
            ("meteorological_ground_decisions", logical, values)
        ]
        for item in normalized:
            input_logical = stable_content_id(
                "ground-decision-input-key",
                {
                    "ground_decision_id": decision_id,
                    "ordinal": item["ordinal"],
                },
            )
            records.append(
                (
                    "meteorological_ground_decision_inputs",
                    input_logical,
                    {
                        "ground_decision_id": decision_id,
                        **item,
                    },
                )
            )
        self._append_records_atomically(records)
        return decision_id

    def append_tracker_forecast(
        self,
        *,
        nowcast_run_id: str,
        tracker_id: str,
        issued_at: float,
        valid_at: float,
        predicted_latitude: float,
        predicted_longitude: float,
        precipitation_probability: float,
        forecast: Mapping[str, Any],
        eta_seconds: float | None = None,
        ground_decision_id: str | None = None,
    ) -> str:
        issue = _number(issued_at, "issued_at")
        valid = _number(valid_at, "valid_at")
        if valid < issue:
            raise ValueError("valid_at moet op of na issued_at liggen")
        coordinates = _coordinates(
            predicted_latitude,
            predicted_longitude,
            allow_none=False,
        )
        probability = _number(
            precipitation_probability,
            "precipitation_probability",
        )
        if probability < 0 or probability > 1:
            raise ValueError("precipitation_probability moet tussen 0 en 1")
        values = {
            "nowcast_run_id": _record_id(
                nowcast_run_id,
                "nowcast_run_id",
            ),
            "ground_decision_id": (
                _record_id(
                    ground_decision_id,
                    "ground_decision_id",
                )
                if ground_decision_id is not None
                else None
            ),
            "tracker_id": _text(tracker_id, "tracker_id", 300),
            "issued_at": issue,
            "valid_at": valid,
            "predicted_latitude": coordinates[0],
            "predicted_longitude": coordinates[1],
            "precipitation_probability": probability,
            "eta_seconds": _optional_number(eta_seconds, "eta_seconds"),
            "forecast_json": _canonical_json(forecast),
        }
        logical = stable_content_id(
            "tracker-forecast-key",
            {
                "nowcast_run_id": values["nowcast_run_id"],
                "tracker_id": values["tracker_id"],
                "valid_at": values["valid_at"],
            },
        )
        return self._append(
            "meteorological_tracker_forecasts",
            logical,
            values,
        )

    def load_tracker_forecast_record(
        self,
        forecast_id: str,
    ) -> dict[str, Any] | None:
        """Load one immutable forecast row after identity and JSON checks."""

        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            row = db.execute(
                """
                SELECT *
                FROM meteorological_tracker_forecasts
                WHERE forecast_id=?
                """,
                (_record_id(forecast_id, "forecast_id"),),
            ).fetchone()
            if row is None:
                return None
            values = {
                "nowcast_run_id": row["nowcast_run_id"],
                "ground_decision_id": row["ground_decision_id"],
                "tracker_id": row["tracker_id"],
                "issued_at": row["issued_at"],
                "valid_at": row["valid_at"],
                "predicted_latitude": row["predicted_latitude"],
                "predicted_longitude": row["predicted_longitude"],
                "precipitation_probability": row[
                    "precipitation_probability"
                ],
                "eta_seconds": row["eta_seconds"],
                "forecast_json": row["forecast_json"],
            }
            expected_hash, expected_id, _ = self._identity(
                "tracker-forecasts",
                str(row["logical_key"]),
                values,
            )
            if (
                expected_hash != str(row["record_hash"])
                or expected_id != str(row["forecast_id"])
            ):
                raise EvidenceConflictError(
                    "tracker forecast faalt immutable identiteitsverificatie"
                )
            try:
                forecast = json.loads(str(row["forecast_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as err:
                raise EvidenceConflictError(
                    "tracker forecast bevat onleesbare JSON"
                ) from err
            if (
                not isinstance(forecast, Mapping)
                or _canonical_json(forecast) != str(row["forecast_json"])
            ):
                raise EvidenceConflictError(
                    "tracker forecast bevat niet-canonieke object-JSON"
                )
            return {
                "forecast_id": str(row["forecast_id"]),
                "nowcast_run_id": str(row["nowcast_run_id"]),
                "ground_decision_id": (
                    str(row["ground_decision_id"])
                    if row["ground_decision_id"] is not None
                    else None
                ),
                "tracker_id": str(row["tracker_id"]),
                "issued_at": float(row["issued_at"]),
                "valid_at": float(row["valid_at"]),
                "predicted_latitude": float(row["predicted_latitude"]),
                "predicted_longitude": float(row["predicted_longitude"]),
                "precipitation_probability": float(
                    row["precipitation_probability"]
                ),
                "eta_seconds": (
                    float(row["eta_seconds"])
                    if row["eta_seconds"] is not None
                    else None
                ),
                "forecast": dict(forecast),
            }

    def append_weather_system_grouping(self, grouping_snapshot: Any) -> str:
        """Persist one already-computed WeatherSystem grouping losslessly.

        This method only validates and records the supplied immutable snapshot.
        It neither groups tracks nor writes descriptive system motion back to
        CellTracks.
        """
        payload = _normalize_weather_system_grouping_snapshot(grouping_snapshot)
        self._validate_weather_system_dependencies(payload)
        policy = payload["policy"]
        grouping_snapshot_id = payload["grouping_snapshot_id"]
        grouping_values = {
            "grouping_snapshot_id": grouping_snapshot_id,
            "valid_at": payload["valid_at"],
            "analysis_grid_id": payload["analysis_grid_id"],
            "policy_id": policy["policy_id"],
            "policy_fingerprint": policy["fingerprint"],
            "policy_json": _canonical_json(policy),
            "previous_grouping_snapshot_id": payload[
                "previous_grouping_snapshot_id"
            ],
            "retired_system_ids_json": _canonical_json(
                payload["retired_system_ids"]
            ),
            "input_track_count": len(payload["input_tracks"]),
            "system_count": len(payload["systems"]),
            "connection_count": len(payload["connections"]),
            "membership_event_count": len(payload["membership_events"]),
            "snapshot_json": _canonical_json(payload),
            "algorithm_version": payload["algorithm_version"],
        }
        grouping_logical_key = stable_content_id(
            "weather-system-grouping-key",
            {"grouping_snapshot_id": grouping_snapshot_id},
        )
        weather_system_grouping_id = self._prospective_record_id(
            "meteorological_weather_system_groupings",
            grouping_logical_key,
            grouping_values,
        )
        records: list[tuple[str, str, Mapping[str, Any]]] = [
            (
                "meteorological_weather_system_groupings",
                grouping_logical_key,
                grouping_values,
            )
        ]

        input_ids: dict[str, str] = {}
        input_payloads: dict[str, Mapping[str, Any]] = {}
        for ordinal, track in enumerate(payload["input_tracks"]):
            track_values = {
                "weather_system_grouping_id": weather_system_grouping_id,
                "track_snapshot_id": track["track_snapshot_id"],
                "track_id": track["track_id"],
                "current_detection_id": track["current_detection_id"],
                "analysis_grid_id": track["analysis_grid_id"],
                "valid_at": track["valid_at"],
                "object_kind": track["object_kind"],
                "current_footprint_json": _canonical_json(
                    track["current_footprint_pixels"]
                ),
                "rain_shield_detection_ids_json": _canonical_json(
                    track["rain_shield_detection_ids"]
                ),
                "x_m": track["x_m"],
                "y_m": track["y_m"],
                "vx_m_s": track["vx_m_s"],
                "vy_m_s": track["vy_m_s"],
                "motion_reference_valid_at": track[
                    "motion_reference_valid_at"
                ],
                "motion_method": track["motion_method"],
                "lifecycle_state": track["lifecycle_state"],
                "parent_track_ids_json": _canonical_json(
                    track["parent_track_ids"]
                ),
                "child_track_ids_json": _canonical_json(
                    track["child_track_ids"]
                ),
                "lineage_event_ids_json": _canonical_json(
                    track["lineage_event_ids"]
                ),
                "identity_alternative_track_ids_json": _canonical_json(
                    track["identity_alternative_track_ids"]
                ),
                "track_payload_json": _canonical_json(track),
                "ordinal": ordinal,
            }
            logical_key = stable_content_id(
                "weather-system-track-input-key",
                {
                    "grouping_snapshot_id": grouping_snapshot_id,
                    "track_id": track["track_id"],
                },
            )
            input_id = self._prospective_record_id(
                "meteorological_weather_system_track_inputs",
                logical_key,
                track_values,
            )
            input_ids[track["track_id"]] = input_id
            input_payloads[track["track_id"]] = track
            records.append(
                (
                    "meteorological_weather_system_track_inputs",
                    logical_key,
                    track_values,
                )
            )

        system_ids: dict[str, str] = {}
        for ordinal, system in enumerate(payload["systems"]):
            motion = system["descriptive_motion"]
            footprint_json = _canonical_json(
                system["aggregate_current_footprint"]
            )
            system_values = {
                "weather_system_grouping_id": weather_system_grouping_id,
                "source_system_snapshot_id": system["system_snapshot_id"],
                "system_id": system["system_id"],
                "valid_at": system["valid_at"],
                "analysis_grid_id": system["analysis_grid_id"],
                "rain_shield_detection_ids_json": _canonical_json(
                    system["rain_shield_detection_ids"]
                ),
                "aggregate_footprint_json": footprint_json,
                "aggregate_footprint_sha256": hashlib.sha256(
                    footprint_json.encode("utf-8")
                ).hexdigest(),
                "descriptive_vx_m_s": motion["vx_m_s"],
                "descriptive_vy_m_s": motion["vy_m_s"],
                "descriptive_motion_method": motion["method"],
                "motion_support_track_ids_json": _canonical_json(
                    motion["supporting_track_ids"]
                ),
                "maximum_member_residual_m_s": motion[
                    "maximum_member_residual_m_s"
                ],
                "classification": system["classification"],
                "system_uncertainty_json": _canonical_json(
                    system["system_uncertainty"]
                ),
                "uncertain_connection_ids_json": _canonical_json(
                    system["uncertain_connection_ids"]
                ),
                "member_count": len(system["member_tracks"]),
                "footprint_pixel_count": len(
                    system["aggregate_current_footprint"]
                ),
                "motion_support_count": len(motion["supporting_track_ids"]),
                "predecessor_count": len(system["predecessor_candidates"]),
                "system_payload_json": _canonical_json(system),
                "algorithm_version": system["algorithm_version"],
                "ordinal": ordinal,
            }
            logical_key = stable_content_id(
                "weather-system-snapshot-key",
                {
                    "grouping_snapshot_id": grouping_snapshot_id,
                    "system_snapshot_id": system["system_snapshot_id"],
                },
            )
            system_record_id = self._prospective_record_id(
                "meteorological_weather_system_snapshots",
                logical_key,
                system_values,
            )
            system_ids[system["system_id"]] = system_record_id
            records.append(
                (
                    "meteorological_weather_system_snapshots",
                    logical_key,
                    system_values,
                )
            )

        for system in payload["systems"]:
            system_record_id = system_ids[system["system_id"]]
            for ordinal, member in enumerate(system["member_tracks"]):
                track_id = member["track_id"]
                member_values = {
                    "weather_system_snapshot_record_id": system_record_id,
                    "weather_system_track_input_id": input_ids[track_id],
                    "track_snapshot_id": member["track_snapshot_id"],
                    "track_id": track_id,
                    "current_detection_id": member["current_detection_id"],
                    "ordinal": ordinal,
                }
                logical_key = stable_content_id(
                    "weather-system-member-key",
                    {
                        "grouping_snapshot_id": grouping_snapshot_id,
                        "system_id": system["system_id"],
                        "track_id": track_id,
                    },
                )
                records.append(
                    (
                        "meteorological_weather_system_members",
                        logical_key,
                        member_values,
                    )
                )
            for ordinal, pixel in enumerate(
                system["aggregate_current_footprint"]
            ):
                pixel_values = {
                    "weather_system_snapshot_record_id": system_record_id,
                    "pixel_row": pixel[0],
                    "pixel_column": pixel[1],
                    "ordinal": ordinal,
                }
                logical_key = stable_content_id(
                    "weather-system-footprint-pixel-key",
                    {
                        "grouping_snapshot_id": grouping_snapshot_id,
                        "system_id": system["system_id"],
                        "pixel": pixel,
                    },
                )
                records.append(
                    (
                        "meteorological_weather_system_footprint_pixels",
                        logical_key,
                        pixel_values,
                    )
                )
            for ordinal, track_id in enumerate(
                system["descriptive_motion"]["supporting_track_ids"]
            ):
                motion_member_values = {
                    "weather_system_snapshot_record_id": system_record_id,
                    "weather_system_track_input_id": input_ids[track_id],
                    "track_id": track_id,
                    "ordinal": ordinal,
                }
                logical_key = stable_content_id(
                    "weather-system-motion-member-key",
                    {
                        "grouping_snapshot_id": grouping_snapshot_id,
                        "system_id": system["system_id"],
                        "track_id": track_id,
                    },
                )
                records.append(
                    (
                        "meteorological_weather_system_motion_members",
                        logical_key,
                        motion_member_values,
                    )
                )
            for ordinal, predecessor in enumerate(
                system["predecessor_candidates"]
            ):
                predecessor_values = {
                    "weather_system_snapshot_record_id": system_record_id,
                    "previous_system_id": predecessor["previous_system_id"],
                    "direct_member_track_ids_json": _canonical_json(
                        predecessor["direct_member_track_ids"]
                    ),
                    "lineage_supported_track_ids_json": _canonical_json(
                        predecessor["lineage_supported_track_ids"]
                    ),
                    "selected": int(predecessor["selected"]),
                    "predecessor_payload_json": _canonical_json(predecessor),
                    "ordinal": ordinal,
                }
                logical_key = stable_content_id(
                    "weather-system-predecessor-key",
                    {
                        "grouping_snapshot_id": grouping_snapshot_id,
                        "system_id": system["system_id"],
                        "previous_system_id": predecessor[
                            "previous_system_id"
                        ],
                    },
                )
                records.append(
                    (
                        "meteorological_weather_system_predecessor_candidates",
                        logical_key,
                        predecessor_values,
                    )
                )

        for ordinal, connection in enumerate(payload["connections"]):
            connection_values = {
                "weather_system_grouping_id": weather_system_grouping_id,
                "source_connection_id": connection["connection_id"],
                "left_track_input_id": input_ids[connection["left_track_id"]],
                "right_track_input_id": input_ids[
                    connection["right_track_id"]
                ],
                "left_track_id": connection["left_track_id"],
                "right_track_id": connection["right_track_id"],
                "shared_rain_shield_detection_ids_json": _canonical_json(
                    connection["shared_rain_shield_detection_ids"]
                ),
                "overlap_pixel_count": connection["overlap_pixel_count"],
                "footprint_gap_m": connection["footprint_gap_m"],
                "valid_time_difference_s": connection[
                    "valid_time_difference_s"
                ],
                "motion_time_difference_s": connection[
                    "motion_time_difference_s"
                ],
                "motion_difference_m_s": connection["motion_difference_m_s"],
                "motion_coherence": connection["motion_coherence"],
                "spatial_relation": connection["spatial_relation"],
                "decision": connection["decision"],
                "reasons_json": _canonical_json(connection["reasons"]),
                "connection_payload_json": _canonical_json(connection),
                "ordinal": ordinal,
            }
            logical_key = stable_content_id(
                "weather-system-connection-key",
                {
                    "grouping_snapshot_id": grouping_snapshot_id,
                    "connection_id": connection["connection_id"],
                },
            )
            records.append(
                (
                    "meteorological_weather_system_connections",
                    logical_key,
                    connection_values,
                )
            )

        for ordinal, event in enumerate(payload["membership_events"]):
            event_values = {
                "weather_system_grouping_id": weather_system_grouping_id,
                "weather_system_snapshot_record_id": system_ids[
                    event["current_system_id"]
                ],
                "source_event_id": event["event_id"],
                "event_type": event["event_type"],
                "current_system_id": event["current_system_id"],
                "selected_predecessor_system_id": event[
                    "selected_predecessor_system_id"
                ],
                "predecessor_system_ids_json": _canonical_json(
                    event["predecessor_system_ids"]
                ),
                "alternative_predecessor_system_ids_json": _canonical_json(
                    event["alternative_predecessor_system_ids"]
                ),
                "current_member_track_ids_json": _canonical_json(
                    event["current_member_track_ids"]
                ),
                "added_track_ids_json": _canonical_json(
                    event["added_track_ids"]
                ),
                "removed_track_ids_json": _canonical_json(
                    event["removed_track_ids"]
                ),
                "member_lineage_event_ids_json": _canonical_json(
                    event["member_lineage_event_ids"]
                ),
                "member_identity_alternative_track_ids_json": _canonical_json(
                    event["member_identity_alternative_track_ids"]
                ),
                "event_payload_json": _canonical_json(event),
                "ordinal": ordinal,
            }
            logical_key = stable_content_id(
                "weather-system-membership-event-key",
                {
                    "grouping_snapshot_id": grouping_snapshot_id,
                    "event_id": event["event_id"],
                },
            )
            records.append(
                (
                    "meteorological_weather_system_membership_events",
                    logical_key,
                    event_values,
                )
            )

        appended = self._append_records_atomically(records)
        return appended[0]

    def load_weather_system_grouping(
        self,
        grouping_snapshot_id: str,
    ) -> dict[str, Any] | None:
        """Load the exact stored grouping payload without recomputing it."""
        self._require_initialized()
        source_id = _record_id(grouping_snapshot_id, "grouping_snapshot_id")
        with self._lock, closing(self._verified_connection()) as db, db:
            row = db.execute(
                """
                SELECT *
                FROM meteorological_weather_system_groupings
                WHERE grouping_snapshot_id=?
                """,
                (source_id,),
            ).fetchone()
            if row is None:
                return None
            excluded = {
                "weather_system_grouping_id",
                "logical_key",
                "record_hash",
                "created_at_ns",
            }
            values = {
                key: row[key]
                for key in row.keys()
                if key not in excluded
            }
            expected_hash, expected_id, _ = self._identity(
                "weather-system-groupings",
                str(row["logical_key"]),
                values,
            )
            if (
                expected_hash != str(row["record_hash"])
                or expected_id != str(row["weather_system_grouping_id"])
            ):
                raise EvidenceConflictError(
                    "WeatherSystem grouping faalt recordidentiteitscontrole"
                )
            try:
                payload = json.loads(str(row["snapshot_json"]))
            except (TypeError, ValueError) as err:
                raise EvidenceConflictError(
                    "WeatherSystem grouping bevat ongeldige snapshot-JSON"
                ) from err
            if _canonical_json(payload) != str(row["snapshot_json"]):
                raise EvidenceConflictError(
                    "WeatherSystem grouping is niet canoniek opgeslagen"
                )
            if self._weather_system_set_mismatch_count(
                db,
                str(row["weather_system_grouping_id"]),
            ):
                raise EvidenceConflictError(
                    "WeatherSystem groupingbundle is onvolledig of inconsistent"
                )
            return payload

    def _validate_weather_system_dependencies(
        self,
        payload: Mapping[str, Any],
    ) -> None:
        """Require every current track view to match immutable base evidence."""
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            for track in payload["input_tracks"]:
                row = db.execute(
                    """
                    SELECT
                        snapshot.track_id,
                        snapshot.detection_id AS snapshot_detection_id,
                        snapshot.valid_at AS snapshot_valid_at,
                        snapshot.lifecycle_state,
                        detection.valid_at AS detection_valid_at,
                        analysis.analysis_grid_id
                    FROM meteorological_cell_track_snapshots AS snapshot
                    JOIN meteorological_detections AS detection
                      ON detection.detection_id=?
                    JOIN meteorological_analysis_frames AS analysis
                      USING(analysis_frame_id)
                    WHERE snapshot.snapshot_id=?
                    """,
                    (
                        track["current_detection_id"],
                        track["track_snapshot_id"],
                    ),
                ).fetchone()
                if row is None:
                    raise EvidenceForeignKeyError(
                        "WeatherSystem track mist snapshot- of detectionevidence"
                    )
                if (
                    str(row["track_id"]) != track["track_id"]
                    or float(row["snapshot_valid_at"]) != track["valid_at"]
                    or str(row["lifecycle_state"])
                    != track["lifecycle_state"]
                ):
                    raise EvidenceConflictError(
                        "WeatherSystem track komt niet overeen met zijn snapshot"
                    )
                snapshot_detection_id = row["snapshot_detection_id"]
                if (
                    snapshot_detection_id is not None
                    and str(snapshot_detection_id)
                    != track["current_detection_id"]
                ):
                    raise EvidenceConflictError(
                        "WeatherSystem current detection wijkt af van snapshot"
                    )
                detection_valid_at = float(row["detection_valid_at"])
                if (
                    detection_valid_at > track["valid_at"]
                    or (
                        track["lifecycle_state"] != "coasting"
                        and detection_valid_at != track["valid_at"]
                    )
                    or str(row["analysis_grid_id"])
                    != track["analysis_grid_id"]
                ):
                    raise EvidenceConflictError(
                        "WeatherSystem detection heeft ander tijd/gridcontract"
                    )

    def load_raw_payload(self, receipt_id: str) -> bytes | None:
        """Load exact provider bytes after verifying length and SHA-256."""
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            row = db.execute(
                """
                SELECT blob.content_sha256, blob.payload, blob.size_bytes
                FROM meteorological_raw_receipts AS receipt
                JOIN meteorological_raw_blobs AS blob USING(content_sha256)
                WHERE receipt.receipt_id=?
                """,
                (_record_id(receipt_id, "receipt_id"),),
            ).fetchone()
            if row is None:
                return None
            payload = bytes(row["payload"])
            if (
                len(payload) != int(row["size_bytes"])
                or hashlib.sha256(payload).hexdigest()
                != str(row["content_sha256"])
            ):
                raise EvidenceConflictError(
                    "raw blob faalt lengte- of SHA-256-verificatie"
                )
            return payload

    def trace_forecast(self, forecast_id: str) -> dict[str, Any]:
        """Trace one forecast through all mandatory radar evidence."""
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            row = db.execute(
                """
                SELECT
                    forecast.forecast_id,
                    forecast.nowcast_run_id,
                    forecast.ground_decision_id,
                    run.source_snapshot_id AS snapshot_id,
                    snapshot.track_id,
                    snapshot.detection_id,
                    analysis.analysis_frame_id,
                    analysis.native_frame_id,
                    native.receipt_id,
                    receipt.content_sha256
                FROM meteorological_tracker_forecasts AS forecast
                JOIN meteorological_nowcast_runs AS run
                    USING(nowcast_run_id)
                JOIN meteorological_cell_track_snapshots AS snapshot
                    ON snapshot.snapshot_id=run.source_snapshot_id
                LEFT JOIN meteorological_detections AS detection
                    USING(detection_id)
                JOIN meteorological_analysis_frames AS analysis
                    ON analysis.analysis_frame_id=run.analysis_frame_id
                JOIN meteorological_native_frames AS native
                    USING(native_frame_id)
                JOIN meteorological_raw_receipts AS receipt
                    USING(receipt_id)
                JOIN meteorological_raw_blobs AS blob
                    USING(content_sha256)
                WHERE forecast.forecast_id=?
                """,
                (_record_id(forecast_id, "forecast_id"),),
            ).fetchone()
            if row is None:
                exists = db.execute(
                    """
                    SELECT 1 FROM meteorological_tracker_forecasts
                    WHERE forecast_id=?
                    """,
                    (forecast_id,),
                ).fetchone()
                if exists is None:
                    raise KeyError(forecast_id)
                raise EvidenceStoreError(
                    "forecast heeft geen volledige raw-provenanceketen"
                )
            result = dict(row)
            if row["ground_decision_id"] is not None:
                ground_rows = db.execute(
                    """
                    SELECT
                        decision.ground_decision_id,
                        decision.reason_json AS decision_reason_json,
                        input.ground_decision_input_id,
                        input.input_role,
                        input.independence_group AS decision_independence_group,
                        input.input_reason_json,
                        input.ordinal,
                        match.ground_match_id,
                        observation.ground_observation_id,
                        observation.receipt_id AS ground_receipt_id,
                        receipt.content_sha256 AS ground_content_sha256,
                        alias.site_alias_id,
                        alias.physical_site_id,
                        site.independence_group
                    FROM meteorological_ground_decisions AS decision
                    JOIN meteorological_ground_decision_inputs AS input
                        USING(ground_decision_id)
                    JOIN meteorological_ground_matches AS match
                        USING(ground_match_id)
                    JOIN meteorological_ground_observations AS observation
                        USING(ground_observation_id)
                    JOIN meteorological_raw_receipts AS receipt
                        USING(receipt_id)
                    JOIN meteorological_raw_blobs AS blob
                        USING(content_sha256)
                    JOIN meteorological_physical_site_aliases AS alias
                        USING(site_alias_id)
                    JOIN meteorological_physical_sites AS site
                        USING(physical_site_id)
                    WHERE decision.ground_decision_id=?
                    ORDER BY input.ordinal
                    """,
                    (row["ground_decision_id"],),
                ).fetchall()
                if not ground_rows:
                    raise EvidenceStoreError(
                        "forecast heeft een onvolledige ground-provenanceketen"
                    )
                decision_reason = json.loads(
                    str(ground_rows[0]["decision_reason_json"])
                )
                if (
                    isinstance(decision_reason, Mapping)
                    and decision_reason.get("provenance_contract")
                    == _GROUND_DECISION_PROVENANCE_CONTRACT
                ):
                    result["ground_provenance_contract"] = (
                        _GROUND_DECISION_PROVENANCE_CONTRACT
                    )
                    result["ground_registry_fingerprint"] = _sha256_hex(
                        decision_reason.get("registry_fingerprint"),
                        "registry_fingerprint",
                    )
                    result["ground_catalog_fingerprint"] = (
                        _optional_sha256_hex(
                            decision_reason.get("catalog_fingerprint"),
                            "catalog_fingerprint",
                        )
                    )
                    result["ground_decision_reason"] = decision_reason.get(
                        "reason"
                    )
                else:
                    # Schema-v3 stores created before this shadow contract
                    # remain readable, but expose that they have no durable
                    # catalog/registry fingerprint.
                    result["ground_provenance_contract"] = None
                    result["ground_registry_fingerprint"] = None
                    result["ground_catalog_fingerprint"] = None
                    result["ground_decision_reason"] = decision_reason
                traced_inputs: list[dict[str, Any]] = []
                for ground_row in ground_rows:
                    traced = dict(ground_row)
                    input_reason = json.loads(
                        str(traced["input_reason_json"])
                    )
                    if (
                        isinstance(input_reason, Mapping)
                        and input_reason.get("provenance_contract")
                        == _GROUND_DECISION_PROVENANCE_CONTRACT
                    ):
                        traced["mapping_fingerprint"] = _sha256_hex(
                            input_reason.get("mapping_fingerprint"),
                            "mapping_fingerprint",
                        )
                        traced["input_reason"] = input_reason.get("reason")
                    else:
                        traced["mapping_fingerprint"] = None
                        traced["input_reason"] = input_reason
                    traced_inputs.append(traced)
                result["ground_inputs"] = traced_inputs
                result["ground"] = dict(traced_inputs[0])
            else:
                result["ground"] = None
                result["ground_inputs"] = []
                result["ground_provenance_contract"] = None
                result["ground_registry_fingerprint"] = None
                result["ground_catalog_fingerprint"] = None
                result["ground_decision_reason"] = None
            return result

    def stats(self) -> dict[str, int]:
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            return self._stats(db)

    def integrity_check(self) -> dict[str, Any]:
        """Audit SQLite, FKs, exact bytes, immutability, and provenance."""
        self._require_initialized()
        with self._lock, closing(self._verified_connection()) as db, db:
            sqlite_messages = [
                str(row[0])
                for row in db.execute("PRAGMA integrity_check").fetchall()
            ]
            foreign_key_rows = [
                {
                    "table": str(row[0]),
                    "rowid": row[1],
                    "parent": str(row[2]),
                    "fk_index": int(row[3]),
                }
                for row in db.execute("PRAGMA foreign_key_check").fetchall()
            ]
            raw_mismatches = self._raw_mismatch_count(db)
            frame_mismatches = self._frame_mismatch_count(db)
            record_identity_mismatches = self._record_identity_mismatch_count(
                db
            )
            broken_forecast_chains = int(
                db.execute(
                    """
                    SELECT count(*)
                    FROM meteorological_tracker_forecasts AS forecast
                    LEFT JOIN meteorological_nowcast_runs AS run
                        USING(nowcast_run_id)
                    LEFT JOIN meteorological_cell_track_snapshots AS snapshot
                        ON snapshot.snapshot_id=run.source_snapshot_id
                    LEFT JOIN meteorological_analysis_frames AS analysis
                        ON analysis.analysis_frame_id=
                            run.analysis_frame_id
                    LEFT JOIN meteorological_native_frames AS native
                        ON native.native_frame_id=analysis.native_frame_id
                    LEFT JOIN meteorological_raw_receipts AS receipt
                        ON receipt.receipt_id=native.receipt_id
                    LEFT JOIN meteorological_raw_blobs AS blob
                        ON blob.content_sha256=receipt.content_sha256
                    WHERE blob.content_sha256 IS NULL
                    """
                ).fetchone()[0]
            )
            broken_ground_chains = int(
                db.execute(
                    """
                    SELECT count(*)
                    FROM meteorological_tracker_forecasts AS forecast
                    LEFT JOIN meteorological_ground_decisions AS decision
                        USING(ground_decision_id)
                    LEFT JOIN meteorological_ground_decision_inputs AS input
                        USING(ground_decision_id)
                    LEFT JOIN meteorological_ground_matches AS match
                        USING(ground_match_id)
                    LEFT JOIN meteorological_ground_observations AS observation
                        USING(ground_observation_id)
                    LEFT JOIN meteorological_raw_receipts AS receipt
                        ON receipt.receipt_id=observation.receipt_id
                    LEFT JOIN meteorological_raw_blobs AS blob
                        ON blob.content_sha256=receipt.content_sha256
                    LEFT JOIN meteorological_physical_site_aliases AS alias
                        ON alias.site_alias_id=observation.site_alias_id
                    LEFT JOIN meteorological_physical_sites AS site
                        USING(physical_site_id)
                    WHERE forecast.ground_decision_id IS NOT NULL
                      AND (
                        decision.ground_decision_id IS NULL
                        OR input.ground_decision_input_id IS NULL
                        OR match.ground_match_id IS NULL
                        OR observation.ground_observation_id IS NULL
                        OR blob.content_sha256 IS NULL
                        OR alias.site_alias_id IS NULL
                        OR site.physical_site_id IS NULL
                      )
                    """
                ).fetchone()[0]
            )
            provenance_relation_mismatches = (
                self._provenance_relation_mismatch_count(db)
            )
            evidence_set_mismatches = self._evidence_set_mismatch_count(db)
            site_alias_overlaps = self._site_alias_overlap_count(db)
            missing_triggers = self._missing_immutable_triggers(db)
            orphan_count = len(foreign_key_rows)
            return {
                "healthy": (
                    sqlite_messages == ["ok"]
                    and orphan_count == 0
                    and raw_mismatches == 0
                    and frame_mismatches == 0
                    and record_identity_mismatches == 0
                    and broken_forecast_chains == 0
                    and broken_ground_chains == 0
                    and provenance_relation_mismatches == 0
                    and evidence_set_mismatches == 0
                    and site_alias_overlaps == 0
                    and not missing_triggers
                ),
                "sqlite": sqlite_messages,
                "foreign_key_violations": foreign_key_rows,
                "orphan_count": orphan_count,
                "raw_content_mismatches": raw_mismatches,
                "frame_content_mismatches": frame_mismatches,
                "record_identity_mismatches": record_identity_mismatches,
                "broken_forecast_provenance_chains": (
                    broken_forecast_chains
                ),
                "broken_ground_provenance_chains": broken_ground_chains,
                "provenance_relation_mismatches": (
                    provenance_relation_mismatches
                ),
                "evidence_set_mismatches": evidence_set_mismatches,
                "site_alias_overlap_conflicts": site_alias_overlaps,
                "missing_immutable_triggers": missing_triggers,
            }

    @classmethod
    def _prospective_record_id(
        cls,
        table: str,
        logical_key: str,
        values: Mapping[str, Any],
    ) -> str:
        if table not in _RECORD_ID_COLUMNS:
            raise ValueError(f"onbekende evidence-tabel {table}")
        namespace = table.removeprefix("meteorological_").replace("_", "-")
        return cls._identity(namespace, logical_key, values)[1]

    def _append_records_atomically(
        self,
        records: Sequence[tuple[str, str, Mapping[str, Any]]],
    ) -> tuple[str, ...]:
        """Append an immutable parent/member bundle in one transaction."""
        self._require_initialized()
        prepared: list[
            tuple[
                str,
                str,
                Mapping[str, Any],
                str,
                str,
                str,
                str,
                tuple[str, ...],
                tuple[Any, ...],
            ]
        ] = []
        for table, logical_key, values in records:
            if table not in _RECORD_ID_COLUMNS:
                raise ValueError(f"onbekende evidence-tabel {table}")
            namespace = table.removeprefix(
                "meteorological_"
            ).replace("_", "-")
            record_hash, record_id, attempted_json = self._identity(
                namespace,
                logical_key,
                values,
            )
            id_column = _RECORD_ID_COLUMNS[table]
            columns = (
                id_column,
                "logical_key",
                "record_hash",
                *values.keys(),
                "created_at_ns",
            )
            parameters = (
                record_id,
                logical_key,
                record_hash,
                *(
                    sqlite3.Binary(value)
                    if isinstance(value, bytes)
                    else value
                    for value in values.values()
                ),
                time.time_ns(),
            )
            prepared.append(
                (
                    table,
                    logical_key,
                    values,
                    record_hash,
                    record_id,
                    attempted_json,
                    id_column,
                    columns,
                    parameters,
                )
            )
        with self._lock, closing(self._verified_connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing_ids: set[str] = set()
                for (
                    table,
                    logical_key,
                    _,
                    record_hash,
                    record_id,
                    attempted_json,
                    id_column,
                    _,
                    _,
                ) in prepared:
                    existing = self._existing(db, table, logical_key)
                    if existing is None:
                        continue
                    if str(existing["record_hash"]) != record_hash:
                        self._record_conflict(
                            db,
                            table,
                            logical_key,
                            str(existing[id_column]),
                            record_id,
                            attempted_json,
                        )
                        db.commit()
                        raise EvidenceConflictError(
                            f"{table} logical_key heeft andere immutable inhoud"
                        )
                    existing_ids.add(record_id)
                for (
                    table,
                    _,
                    _,
                    _,
                    record_id,
                    _,
                    _,
                    columns,
                    parameters,
                ) in prepared:
                    if record_id in existing_ids:
                        continue
                    db.execute(self._insert_sql(table, columns), parameters)
                db.commit()
                return tuple(item[4] for item in prepared)
            except EvidenceConflictError:
                if db.in_transaction:
                    db.rollback()
                raise
            except sqlite3.IntegrityError as err:
                db.rollback()
                self._raise_integrity(err)
            except BaseException:
                db.rollback()
                raise

    def _append(
        self,
        table: str,
        logical_key: str,
        values: Mapping[str, Any],
    ) -> str:
        self._require_initialized()
        if table not in _RECORD_ID_COLUMNS:
            raise ValueError(f"onbekende evidence-tabel {table}")
        namespace = table.removeprefix("meteorological_").replace("_", "-")
        record_hash, record_id, attempted_json = self._identity(
            namespace,
            logical_key,
            values,
        )
        id_column = _RECORD_ID_COLUMNS[table]
        columns = (
            id_column,
            "logical_key",
            "record_hash",
            *values.keys(),
            "created_at_ns",
        )
        parameters = (
            record_id,
            logical_key,
            record_hash,
            *(
                sqlite3.Binary(value) if isinstance(value, bytes) else value
                for value in values.values()
            ),
            time.time_ns(),
        )
        with self._lock, closing(self._verified_connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = self._existing(db, table, logical_key)
                if existing is not None:
                    if str(existing["record_hash"]) == record_hash:
                        db.commit()
                        return str(existing[id_column])
                    self._record_conflict(
                        db,
                        table,
                        logical_key,
                        str(existing[id_column]),
                        record_id,
                        attempted_json,
                    )
                    db.commit()
                    raise EvidenceConflictError(
                        f"{table} logical_key heeft andere immutable inhoud"
                    )
                db.execute(self._insert_sql(table, columns), parameters)
                db.commit()
                return record_id
            except EvidenceConflictError:
                if db.in_transaction:
                    db.rollback()
                raise
            except sqlite3.IntegrityError as err:
                db.rollback()
                self._raise_integrity(err)
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _identity(
        namespace: str,
        logical_key: str,
        values: Mapping[str, Any],
    ) -> tuple[str, str, str]:
        canonical_values = {
            key: (
                {
                    "sha256": hashlib.sha256(value).hexdigest(),
                    "size_bytes": len(value),
                }
                if isinstance(value, bytes)
                else value
            )
            for key, value in values.items()
        }
        record = {
            "logical_key": logical_key,
            "values": canonical_values,
        }
        attempted_json = _canonical_json(record)
        record_hash = hashlib.sha256(
            attempted_json.encode("utf-8")
        ).hexdigest()
        record_id = stable_content_id(namespace, record)
        return record_hash, record_id, attempted_json

    @staticmethod
    def _existing(
        db: sqlite3.Connection,
        table: str,
        logical_key: str,
    ) -> sqlite3.Row | None:
        return db.execute(
            f"SELECT * FROM {table} WHERE logical_key=?",
            (logical_key,),
        ).fetchone()

    @staticmethod
    def _insert_sql(table: str, columns: Sequence[str]) -> str:
        column_sql = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        return f"INSERT INTO {table}({column_sql}) VALUES({placeholders})"

    @staticmethod
    def _record_conflict(
        db: sqlite3.Connection,
        table: str,
        logical_key: str,
        existing_record_id: str,
        attempted_record_id: str,
        attempted_json: str,
    ) -> None:
        conflict_id = stable_content_id(
            "evidence-conflict",
            {
                "table": table,
                "logical_key": logical_key,
                "existing_record_id": existing_record_id,
                "attempted_record_id": attempted_record_id,
            },
        )
        existing = db.execute(
            """
            SELECT 1 FROM meteorological_evidence_conflict_events
            WHERE conflict_id=?
            """,
            (conflict_id,),
        ).fetchone()
        if existing is None:
            db.execute(
                """
                INSERT INTO meteorological_evidence_conflict_events(
                    conflict_id, table_name, logical_key,
                    existing_record_id, attempted_record_id,
                    attempted_record_json, detected_at_ns
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conflict_id,
                    table,
                    logical_key,
                    existing_record_id,
                    attempted_record_id,
                    attempted_json,
                    time.time_ns(),
                ),
            )

    def _create_schema(self, db: sqlite3.Connection) -> None:
        statements = ["BEGIN IMMEDIATE"]
        statements.extend(_TABLE_DEFINITIONS.values())
        statements.extend(
            (
                f"""
                CREATE TRIGGER immutable_{table}_update
                BEFORE UPDATE ON {table}
                BEGIN
                    SELECT RAISE(ABORT, 'append-only evidence table');
                END
                """,
                f"""
                CREATE TRIGGER immutable_{table}_delete
                BEFORE DELETE ON {table}
                BEGIN
                    SELECT RAISE(ABORT, 'append-only evidence table');
                END
                """,
            )
            for table in _IMMUTABLE_TABLES
        )
        flattened: list[str] = []
        for statement in statements:
            if isinstance(statement, tuple):
                flattened.extend(statement)
            else:
                flattened.append(statement)
        flattened.extend(
            [
                """
                INSERT INTO meteorological_evidence_metadata(
                    metadata_key, metadata_value
                ) VALUES('store_kind', 'storm-tracker-meteorological-evidence')
                """,
                f"""
                INSERT INTO meteorological_evidence_metadata(
                    metadata_key, metadata_value
                ) VALUES(
                    'schema_version',
                    '{METEOROLOGICAL_EVIDENCE_SCHEMA_VERSION}'
                )
                """,
                """
                INSERT INTO meteorological_evidence_schema_migrations(
                    migration_id, from_version, to_version, applied_at_ns
                ) VALUES('empty-to-v3', 0, 3, 0)
                """,
                "COMMIT",
            ]
        )
        script = ";\n".join(statement.strip() for statement in flattened) + ";"
        try:
            db.executescript(script)
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise

    def _migrate_v2_to_v3(
        self,
        db: sqlite3.Connection,
        tables: set[str],
    ) -> None:
        """Add only the WeatherSystem ledger tables to an exact v2 store."""
        self._validate_v2_schema(db, tables)
        self._configure_durability(db)
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """
                DROP TRIGGER
                    immutable_meteorological_evidence_metadata_update
                """
            )
            for table in _WEATHER_SYSTEM_TABLES:
                db.execute(_TABLE_DEFINITIONS[table])
                for operation in ("update", "delete"):
                    db.execute(
                        f"""
                        CREATE TRIGGER immutable_{table}_{operation}
                        BEFORE {operation.upper()} ON {table}
                        BEGIN
                            SELECT RAISE(
                                ABORT,
                                'append-only evidence table'
                            );
                        END
                        """
                    )
            db.execute(
                """
                UPDATE meteorological_evidence_metadata
                SET metadata_value='3'
                WHERE metadata_key='schema_version'
                """
            )
            if db.execute("SELECT changes()").fetchone()[0] != 1:
                raise EvidenceSchemaError(
                    "v2 evidence-store mist exact één schema_version-record"
                )
            db.execute(
                """
                INSERT INTO meteorological_evidence_schema_migrations(
                    migration_id, from_version, to_version, applied_at_ns
                ) VALUES('v2-to-v3', 2, 3, ?)
                """,
                (time.time_ns(),),
            )
            db.execute(
                """
                CREATE TRIGGER
                    immutable_meteorological_evidence_metadata_update
                BEFORE UPDATE ON meteorological_evidence_metadata
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'append-only evidence table'
                    );
                END
                """
            )
            db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise

    def _validate_v2_schema(
        self,
        db: sqlite3.Connection,
        tables: set[str],
    ) -> None:
        """Prove that an existing database is the exact supported v2."""
        if tables != _V2_EXPECTED_TABLES:
            raise EvidenceSchemaError(
                "database is niet het exacte ondersteunde v2-schema"
            )
        metadata = {
            str(row["metadata_key"]): str(row["metadata_value"])
            for row in db.execute(
                """
                SELECT metadata_key, metadata_value
                FROM meteorological_evidence_metadata
                """
            ).fetchall()
        }
        if metadata != {
            "store_kind": _STORE_KIND,
            "schema_version": "2",
        }:
            raise EvidenceSchemaError(
                "database heeft geen exacte v2 evidence-store-identiteit"
            )
        migration_history = {
            (
                str(row["migration_id"]),
                int(row["from_version"]),
                int(row["to_version"]),
            )
            for row in db.execute(
                """
                SELECT migration_id, from_version, to_version
                FROM meteorological_evidence_schema_migrations
                """
            ).fetchall()
        }
        if migration_history != {("empty-to-v2", 0, 2)}:
            raise EvidenceSchemaError(
                "v2 evidence-store heeft onbekende migratiehistorie"
            )
        for table in _V2_IMMUTABLE_TABLES:
            expected = _columns_from_definition(_TABLE_DEFINITIONS[table])
            actual = {
                str(row["name"])
                for row in db.execute(f"PRAGMA table_info({table})")
            }
            if actual != expected:
                raise EvidenceSchemaError(
                    f"v2 evidence-tabel {table} heeft onverwachte kolommen"
                )
        missing_triggers = self._missing_immutable_triggers(
            db,
            _V2_IMMUTABLE_TABLES,
        )
        if missing_triggers:
            raise EvidenceSchemaError(
                "v2 evidence-store mist immutable triggers: "
                + ", ".join(missing_triggers)
            )
        integrity = [
            str(row[0])
            for row in db.execute("PRAGMA integrity_check").fetchall()
        ]
        if integrity != ["ok"]:
            raise EvidenceSchemaError(
                "v2 evidence-store faalt SQLite integrity_check: "
                + "; ".join(integrity)
            )
        if db.execute("PRAGMA foreign_key_check").fetchall():
            raise EvidenceSchemaError(
                "v2 evidence-store bevat foreign-key-overtredingen"
            )

    def _validate_schema(
        self,
        db: sqlite3.Connection,
        tables: set[str],
    ) -> None:
        if tables != _EXPECTED_TABLES:
            raise EvidenceSchemaError(
                "onbekend of onvolledig meteorological-evidenceschema; "
                f"gevonden tabellen: {sorted(tables)}"
            )
        metadata = {
            str(row["metadata_key"]): str(row["metadata_value"])
            for row in db.execute(
                """
                SELECT metadata_key, metadata_value
                FROM meteorological_evidence_metadata
                """
            ).fetchall()
        }
        if metadata.get("store_kind") != _STORE_KIND:
            raise EvidenceSchemaError(
                "database heeft geen geldige evidence-store-identiteit"
            )
        raw_version = metadata.get("schema_version")
        try:
            version = int(raw_version) if raw_version is not None else -1
        except ValueError as err:
            raise EvidenceSchemaError(
                "evidence-store heeft een ongeldige schemaversie"
            ) from err
        if version != METEOROLOGICAL_EVIDENCE_SCHEMA_VERSION:
            raise EvidenceSchemaError(
                "onbekende meteorological-evidenceschemaversie "
                f"{version}; verwacht "
                f"{METEOROLOGICAL_EVIDENCE_SCHEMA_VERSION}"
            )
        migration_history = {
            (
                str(row["migration_id"]),
                int(row["from_version"]),
                int(row["to_version"]),
            )
            for row in db.execute(
            """
            SELECT migration_id, from_version, to_version
            FROM meteorological_evidence_schema_migrations
            """
            ).fetchall()
        }
        accepted_histories = (
            {("empty-to-v3", 0, 3)},
            {
                ("empty-to-v2", 0, 2),
                ("v2-to-v3", 2, 3),
            },
        )
        if migration_history not in accepted_histories:
            raise EvidenceSchemaError(
                "evidence-store heeft onbekende migratiehistorie"
            )
        for table, definition in _TABLE_DEFINITIONS.items():
            expected = _columns_from_definition(definition)
            actual = {
                str(row["name"])
                for row in db.execute(f"PRAGMA table_info({table})")
            }
            if actual != expected:
                raise EvidenceSchemaError(
                    f"evidence-tabel {table} heeft onverwachte kolommen"
                )
        missing_triggers = self._missing_immutable_triggers(db)
        if missing_triggers:
            raise EvidenceSchemaError(
                "evidence-store mist immutable triggers: "
                + ", ".join(missing_triggers)
            )
        integrity = [
            str(row[0])
            for row in db.execute("PRAGMA integrity_check").fetchall()
        ]
        if integrity != ["ok"]:
            raise EvidenceSchemaError(
                "evidence-store faalt SQLite integrity_check: "
                + "; ".join(integrity)
            )
        foreign_keys = db.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_keys:
            raise EvidenceSchemaError(
                "evidence-store bevat foreign-key-overtredingen"
            )

    @staticmethod
    def _table_names(db: sqlite3.Connection) -> set[str]:
        return {
            str(row["name"])
            for row in db.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }

    @staticmethod
    def _missing_immutable_triggers(
        db: sqlite3.Connection,
        tables: Sequence[str] | None = None,
    ) -> list[str]:
        present = {
            str(row["name"]): str(row["sql"] or "")
            for row in db.execute(
                """
                SELECT name, sql FROM sqlite_master WHERE type='trigger'
                """
            ).fetchall()
        }
        selected_tables = (
            _IMMUTABLE_TABLES
            if tables is None
            else tuple(tables)
        )
        expected = {
            f"immutable_{table}_{operation}": (
                f"""
                CREATE TRIGGER immutable_{table}_{operation}
                BEFORE {operation.upper()} ON {table}
                BEGIN
                    SELECT RAISE(ABORT, 'append-only evidence table');
                END
                """
            )
            for table in selected_tables
            for operation in ("update", "delete")
        }
        invalid = {
            name
            for name, sql in expected.items()
            if name not in present
            or "".join(present[name].lower().split())
            != "".join(sql.lower().split())
        }
        invalid.update(
            f"unexpected:{name}"
            for name in set(present) - set(expected)
        )
        return sorted(invalid)

    @staticmethod
    def _raw_mismatch_count(db: sqlite3.Connection) -> int:
        mismatches = 0
        for row in db.execute(
            """
            SELECT content_sha256, payload, size_bytes
            FROM meteorological_raw_blobs
            """
        ):
            payload = bytes(row["payload"])
            if (
                len(payload) != int(row["size_bytes"])
                or hashlib.sha256(payload).hexdigest()
                != str(row["content_sha256"])
            ):
                mismatches += 1
        return mismatches

    @staticmethod
    def _frame_mismatch_count(db: sqlite3.Connection) -> int:
        mismatches = 0
        for table in (
            "meteorological_native_frames",
            "meteorological_analysis_frames",
        ):
            for row in db.execute(
                f"SELECT frame_sha256, frame_payload FROM {table}"
            ):
                if hashlib.sha256(bytes(row["frame_payload"])).hexdigest() != str(
                    row["frame_sha256"]
                ):
                    mismatches += 1
        for row in db.execute(
            """
            SELECT field_sha256, field_payload
            FROM meteorological_flow_runs
            """
        ):
            if hashlib.sha256(bytes(row["field_payload"])).hexdigest() != str(
                row["field_sha256"]
            ):
                mismatches += 1
        return mismatches

    @classmethod
    def _record_identity_mismatch_count(
        cls,
        db: sqlite3.Connection,
    ) -> int:
        mismatches = 0
        excluded = {
            "logical_key",
            "record_hash",
            "created_at_ns",
        }
        for table, id_column in _RECORD_ID_COLUMNS.items():
            columns = [
                str(row["name"])
                for row in db.execute(f"PRAGMA table_info({table})")
            ]
            value_columns = [
                column
                for column in columns
                if column not in excluded and column != id_column
            ]
            for row in db.execute(f"SELECT * FROM {table}"):
                values = {
                    column: (
                        bytes(row[column])
                        if isinstance(row[column], bytes)
                        else row[column]
                    )
                    for column in value_columns
                }
                namespace = table.removeprefix(
                    "meteorological_"
                ).replace("_", "-")
                expected_hash, expected_id, _ = cls._identity(
                    namespace,
                    str(row["logical_key"]),
                    values,
                )
                if (
                    expected_hash != str(row["record_hash"])
                    or expected_id != str(row[id_column])
                ):
                    mismatches += 1
        return mismatches

    @staticmethod
    def _provenance_relation_mismatch_count(
        db: sqlite3.Connection,
    ) -> int:
        checks = (
            """
            SELECT count(*)
            FROM meteorological_nowcast_runs AS run
            JOIN meteorological_cell_track_snapshots AS snapshot
                ON snapshot.snapshot_id=run.source_snapshot_id
            JOIN meteorological_detections AS detection
                USING(detection_id)
            WHERE snapshot.detection_id IS NOT NULL
              AND run.analysis_frame_id <> detection.analysis_frame_id
            """,
            """
            SELECT count(*)
            FROM meteorological_motion_estimates AS motion
            LEFT JOIN meteorological_flow_samples AS sample
                USING(flow_sample_id)
            LEFT JOIN meteorological_flow_runs AS sample_run
                ON sample_run.flow_run_id=sample.flow_run_id
            LEFT JOIN meteorological_flow_runs AS motion_run
                ON motion_run.flow_run_id=motion.flow_run_id
            JOIN meteorological_detections AS source_detection
                ON source_detection.detection_id=motion.from_detection_id
            JOIN meteorological_detections AS target_detection
                ON target_detection.detection_id=motion.to_detection_id
            WHERE (
                motion.flow_sample_id IS NOT NULL
                AND (
                    motion.flow_run_id IS NULL
                    OR sample.flow_sample_id IS NULL
                    OR sample.flow_run_id<>motion.flow_run_id
                )
              )
              OR (
                motion.flow_run_id IS NOT NULL
                AND (
                    motion_run.flow_run_id IS NULL
                    OR motion_run.from_analysis_frame_id
                        <> source_detection.analysis_frame_id
                    OR motion_run.to_analysis_frame_id
                        <> target_detection.analysis_frame_id
                )
              )
            """,
            """
            SELECT count(*)
            FROM meteorological_association_candidates AS candidate
            JOIN meteorological_motion_estimates AS motion USING(motion_id)
            WHERE candidate.motion_id IS NOT NULL
              AND (
                candidate.from_detection_id <> motion.from_detection_id
                OR candidate.to_detection_id <> motion.to_detection_id
              )
            """,
            """
            SELECT count(*)
            FROM meteorological_cell_track_snapshots AS snapshot
            JOIN meteorological_association_decisions AS decision
                USING(association_decision_id)
            JOIN meteorological_association_candidates AS candidate
                USING(candidate_id)
            LEFT JOIN meteorological_cell_track_snapshots AS previous
                ON previous.snapshot_id=snapshot.previous_snapshot_id
            WHERE snapshot.association_decision_id IS NOT NULL
              AND (
                candidate.to_detection_id <> snapshot.detection_id
                OR (
                    previous.snapshot_id IS NOT NULL
                    AND candidate.from_detection_id
                        <> previous.detection_id
                )
              )
            """,
            """
            SELECT count(*)
            FROM meteorological_association_decisions AS decision
            LEFT JOIN meteorological_assignment_solution_members AS member
              ON member.assignment_solution_id=
                    decision.assignment_solution_id
             AND member.candidate_id=decision.candidate_id
            WHERE decision.assignment_solution_id IS NOT NULL
              AND member.assignment_member_id IS NULL
            """,
            """
            SELECT count(*)
            FROM meteorological_track_lineage_members AS member
            JOIN meteorological_cell_track_snapshots AS snapshot
                USING(snapshot_id)
            WHERE snapshot.track_id <> member.track_id
            """,
            """
            SELECT count(*)
            FROM meteorological_tracker_forecasts AS forecast
            JOIN meteorological_nowcast_runs AS run USING(nowcast_run_id)
            JOIN meteorological_cell_track_snapshots AS forecast_snapshot
                ON forecast_snapshot.snapshot_id=run.source_snapshot_id
            JOIN meteorological_ground_decisions AS decision
                USING(ground_decision_id)
            JOIN meteorological_ground_decision_inputs AS input
                USING(ground_decision_id)
            JOIN meteorological_ground_matches AS ground_match
                USING(ground_match_id)
            JOIN meteorological_cell_track_snapshots AS ground_snapshot
                ON ground_snapshot.snapshot_id=ground_match.snapshot_id
            WHERE forecast.ground_decision_id IS NOT NULL
              AND ground_snapshot.track_id <> forecast_snapshot.track_id
            """,
            """
            SELECT count(*)
            FROM meteorological_ground_decision_inputs AS input
            JOIN meteorological_ground_matches AS match
                USING(ground_match_id)
            JOIN meteorological_ground_observations AS observation
                USING(ground_observation_id)
            JOIN meteorological_physical_site_aliases AS alias
                USING(site_alias_id)
            JOIN meteorological_physical_sites AS site
                USING(physical_site_id)
            WHERE input.independence_group <> site.independence_group
            """,
            """
            SELECT count(*)
            FROM meteorological_assignment_run_candidates AS member
            JOIN meteorological_assignment_runs AS run
                USING(assignment_run_id)
            JOIN meteorological_association_candidates AS candidate
                USING(candidate_id)
            JOIN meteorological_detections AS source_detection
                ON source_detection.detection_id=
                    candidate.from_detection_id
            JOIN meteorological_detections AS target_detection
                ON target_detection.detection_id=
                    candidate.to_detection_id
            WHERE source_detection.analysis_frame_id
                    <> run.from_analysis_frame_id
               OR target_detection.analysis_frame_id
                    <> run.to_analysis_frame_id
            """,
            """
            SELECT count(*)
            FROM meteorological_assignment_solution_members AS member
            JOIN meteorological_assignment_solutions AS solution
                USING(assignment_solution_id)
            JOIN meteorological_assignment_runs AS run
                USING(assignment_run_id)
            LEFT JOIN meteorological_assignment_run_candidates AS listed
                ON listed.assignment_run_id=run.assignment_run_id
               AND listed.candidate_id=member.candidate_id
            LEFT JOIN meteorological_detections AS detection
                USING(detection_id)
            WHERE (
                member.candidate_id IS NOT NULL
                AND listed.run_candidate_id IS NULL
              )
              OR (
                member.member_role='birth'
                AND detection.analysis_frame_id
                    <> run.to_analysis_frame_id
              )
              OR (
                member.member_role='death'
                AND detection.analysis_frame_id
                    <> run.from_analysis_frame_id
              )
            """,
            """
            SELECT count(*)
            FROM meteorological_source_revision_events AS event
            JOIN meteorological_raw_receipts AS prior
                ON prior.receipt_id=event.prior_receipt_id
            JOIN meteorological_raw_receipts AS revised
                ON revised.receipt_id=event.revised_receipt_id
            WHERE prior.provider_id<>event.provider_id
               OR revised.provider_id<>event.provider_id
               OR prior.upstream_record_id<>event.upstream_record_id
               OR revised.upstream_record_id<>event.upstream_record_id
               OR prior.content_sha256<>event.prior_content_sha256
               OR revised.content_sha256<>event.revised_content_sha256
            """,
            """
            WITH ordered AS (
                SELECT
                    receipt_id AS revised_receipt_id,
                    content_sha256 AS revised_content_sha256,
                    lag(receipt_id) OVER (
                        PARTITION BY provider_id, upstream_record_id
                        ORDER BY created_at_ns, receipt_id
                    ) AS prior_receipt_id,
                    lag(content_sha256) OVER (
                        PARTITION BY provider_id, upstream_record_id
                        ORDER BY created_at_ns, receipt_id
                    ) AS prior_content_sha256
                FROM meteorological_raw_receipts
            )
            SELECT count(*)
            FROM ordered
            LEFT JOIN meteorological_source_revision_events AS event
              ON event.prior_receipt_id=ordered.prior_receipt_id
             AND event.revised_receipt_id=ordered.revised_receipt_id
            WHERE ordered.prior_receipt_id IS NOT NULL
              AND ordered.prior_content_sha256
                    <> ordered.revised_content_sha256
              AND event.source_revision_event_id IS NULL
            """,
            """
            WITH transitions AS (
                SELECT
                    receipt_id AS revised_receipt_id,
                    lag(receipt_id) OVER (
                        PARTITION BY provider_id, upstream_record_id
                        ORDER BY created_at_ns, receipt_id
                    ) AS prior_receipt_id,
                    content_sha256 AS revised_content_sha256,
                    lag(content_sha256) OVER (
                        PARTITION BY provider_id, upstream_record_id
                        ORDER BY created_at_ns, receipt_id
                    ) AS prior_content_sha256
                FROM meteorological_raw_receipts
            )
            SELECT count(*)
            FROM meteorological_source_revision_events AS event
            LEFT JOIN transitions
              ON transitions.prior_receipt_id=event.prior_receipt_id
             AND transitions.revised_receipt_id=event.revised_receipt_id
             AND transitions.prior_content_sha256
                    <> transitions.revised_content_sha256
            WHERE transitions.revised_receipt_id IS NULL
            """,
            """
            SELECT count(*)
            FROM meteorological_weather_system_snapshots AS system
            JOIN meteorological_weather_system_groupings AS grouping
              USING(weather_system_grouping_id)
            WHERE system.valid_at<>grouping.valid_at
               OR system.analysis_grid_id<>grouping.analysis_grid_id
               OR system.algorithm_version<>grouping.algorithm_version
            """,
            """
            SELECT count(*)
            FROM meteorological_weather_system_track_inputs AS input
            JOIN meteorological_weather_system_groupings AS grouping
              USING(weather_system_grouping_id)
            JOIN meteorological_cell_track_snapshots AS snapshot
              ON snapshot.snapshot_id=input.track_snapshot_id
            JOIN meteorological_detections AS detection
              ON detection.detection_id=input.current_detection_id
            JOIN meteorological_analysis_frames AS analysis
              USING(analysis_frame_id)
            WHERE input.valid_at<>grouping.valid_at
               OR input.analysis_grid_id<>grouping.analysis_grid_id
               OR snapshot.track_id<>input.track_id
               OR snapshot.valid_at<>input.valid_at
               OR snapshot.lifecycle_state<>input.lifecycle_state
               OR (
                    snapshot.detection_id IS NOT NULL
                    AND snapshot.detection_id<>input.current_detection_id
               )
               OR detection.valid_at>input.valid_at
               OR (
                    input.lifecycle_state<>'coasting'
                    AND detection.valid_at<>input.valid_at
               )
               OR analysis.analysis_grid_id<>input.analysis_grid_id
            """,
            """
            SELECT count(*)
            FROM meteorological_weather_system_members AS member
            JOIN meteorological_weather_system_snapshots AS system
              USING(weather_system_snapshot_record_id)
            JOIN meteorological_weather_system_track_inputs AS input
              USING(weather_system_track_input_id)
            WHERE system.weather_system_grouping_id
                    <> input.weather_system_grouping_id
               OR member.track_snapshot_id<>input.track_snapshot_id
               OR member.track_id<>input.track_id
               OR member.current_detection_id<>input.current_detection_id
            """,
            """
            SELECT count(*)
            FROM meteorological_weather_system_motion_members AS motion
            JOIN meteorological_weather_system_snapshots AS system
              USING(weather_system_snapshot_record_id)
            JOIN meteorological_weather_system_track_inputs AS input
              USING(weather_system_track_input_id)
            LEFT JOIN meteorological_weather_system_members AS member
              ON member.weather_system_snapshot_record_id=
                    motion.weather_system_snapshot_record_id
             AND member.weather_system_track_input_id=
                    motion.weather_system_track_input_id
            WHERE system.weather_system_grouping_id
                    <> input.weather_system_grouping_id
               OR motion.track_id<>input.track_id
               OR member.weather_system_member_id IS NULL
            """,
            """
            SELECT count(*)
            FROM meteorological_weather_system_connections AS connection
            JOIN meteorological_weather_system_track_inputs AS left_input
              ON left_input.weather_system_track_input_id=
                    connection.left_track_input_id
            JOIN meteorological_weather_system_track_inputs AS right_input
              ON right_input.weather_system_track_input_id=
                    connection.right_track_input_id
            WHERE left_input.weather_system_grouping_id
                    <> connection.weather_system_grouping_id
               OR right_input.weather_system_grouping_id
                    <> connection.weather_system_grouping_id
               OR left_input.track_id<>connection.left_track_id
               OR right_input.track_id<>connection.right_track_id
            """,
            """
            SELECT count(*)
            FROM meteorological_weather_system_membership_events AS event
            JOIN meteorological_weather_system_snapshots AS system
              USING(weather_system_snapshot_record_id)
            WHERE system.weather_system_grouping_id
                    <> event.weather_system_grouping_id
               OR system.system_id<>event.current_system_id
            """,
        )
        return sum(
            int(db.execute(statement).fetchone()[0])
            for statement in checks
        )

    @staticmethod
    def _evidence_set_mismatch_count(db: sqlite3.Connection) -> int:
        """Verify parent-declared member counts and canonical set digests."""
        mismatches = 0
        specifications = (
            (
                "meteorological_assignment_runs",
                "assignment_run_id",
                "candidate_count",
                "candidate_set_sha256",
                """
                SELECT candidate_id
                FROM meteorological_assignment_run_candidates
                WHERE assignment_run_id=?
                ORDER BY candidate_rank
                """,
                lambda rows: [str(row["candidate_id"]) for row in rows],
            ),
            (
                "meteorological_assignment_solutions",
                "assignment_solution_id",
                "member_count",
                "member_set_sha256",
                """
                SELECT member_role, candidate_id, detection_id,
                       alternative_rank, member_cost, rationale_json,
                       ordinal
                FROM meteorological_assignment_solution_members
                WHERE assignment_solution_id=?
                ORDER BY ordinal
                """,
                lambda rows: [dict(row) for row in rows],
            ),
            (
                "meteorological_track_lineage_events",
                "lineage_event_id",
                "member_count",
                "member_set_sha256",
                """
                SELECT member_role, track_id, snapshot_id, ordinal,
                       member_detail_json
                FROM meteorological_track_lineage_members
                WHERE lineage_event_id=?
                ORDER BY ordinal
                """,
                lambda rows: [dict(row) for row in rows],
            ),
            (
                "meteorological_ground_decisions",
                "ground_decision_id",
                "input_count",
                "input_set_sha256",
                """
                SELECT ground_match_id, input_role, independence_group,
                       input_reason_json, ordinal
                FROM meteorological_ground_decision_inputs
                WHERE ground_decision_id=?
                ORDER BY ordinal
                """,
                lambda rows: [dict(row) for row in rows],
            ),
        )
        for (
            parent_table,
            id_column,
            count_column,
            digest_column,
            member_query,
            normalize,
        ) in specifications:
            for parent in db.execute(
                f"""
                SELECT {id_column}, {count_column}, {digest_column}
                FROM {parent_table}
                """
            ):
                rows = db.execute(
                    member_query,
                    (parent[id_column],),
                ).fetchall()
                normalized = normalize(rows)
                digest = hashlib.sha256(
                    _canonical_json(normalized).encode("utf-8")
                ).hexdigest()
                if (
                    len(rows) != int(parent[count_column])
                    or digest != str(parent[digest_column])
                ):
                    mismatches += 1
        for event in db.execute(
            """
            SELECT event.lineage_event_id,
                   count(DISTINCT member.member_role) AS role_count
            FROM meteorological_track_lineage_events AS event
            LEFT JOIN meteorological_track_lineage_members AS member
                USING(lineage_event_id)
            GROUP BY event.lineage_event_id
            """
        ):
            if int(event["role_count"]) != 2:
                mismatches += 1
        mismatches += (
            MeteorologicalEvidenceStore._weather_system_set_mismatch_count(db)
        )
        return mismatches

    @staticmethod
    def _weather_system_set_mismatch_count(
        db: sqlite3.Connection,
        weather_system_grouping_id: str | None = None,
    ) -> int:
        """Verify every normalized WeatherSystem child row against its snapshot."""

        def decode(value: Any) -> Any:
            decoded = json.loads(str(value))
            if _canonical_json(decoded) != str(value):
                raise ValueError("niet-canonieke WeatherSystem JSON")
            return decoded

        query = "SELECT * FROM meteorological_weather_system_groupings"
        parameters: tuple[Any, ...] = ()
        if weather_system_grouping_id is not None:
            query += " WHERE weather_system_grouping_id=?"
            parameters = (weather_system_grouping_id,)
        mismatches = 0
        for grouping in db.execute(query, parameters):
            try:
                payload = decode(grouping["snapshot_json"])
                grouping_id = str(grouping["weather_system_grouping_id"])
                if (
                    decode(grouping["policy_json"]) != payload["policy"]
                    or decode(grouping["retired_system_ids_json"])
                    != payload["retired_system_ids"]
                ):
                    raise ValueError("groupingmetadata wijkt af")

                track_rows = db.execute(
                    """
                    SELECT track_payload_json
                    FROM meteorological_weather_system_track_inputs
                    WHERE weather_system_grouping_id=?
                    ORDER BY ordinal
                    """,
                    (grouping_id,),
                ).fetchall()
                system_rows = db.execute(
                    """
                    SELECT *
                    FROM meteorological_weather_system_snapshots
                    WHERE weather_system_grouping_id=?
                    ORDER BY ordinal
                    """,
                    (grouping_id,),
                ).fetchall()
                connection_rows = db.execute(
                    """
                    SELECT connection_payload_json
                    FROM meteorological_weather_system_connections
                    WHERE weather_system_grouping_id=?
                    ORDER BY ordinal
                    """,
                    (grouping_id,),
                ).fetchall()
                event_rows = db.execute(
                    """
                    SELECT event_payload_json
                    FROM meteorological_weather_system_membership_events
                    WHERE weather_system_grouping_id=?
                    ORDER BY ordinal
                    """,
                    (grouping_id,),
                ).fetchall()
                if (
                    int(grouping["input_track_count"]) != len(track_rows)
                    or int(grouping["system_count"]) != len(system_rows)
                    or int(grouping["connection_count"])
                    != len(connection_rows)
                    or int(grouping["membership_event_count"])
                    != len(event_rows)
                    or [decode(row[0]) for row in track_rows]
                    != payload["input_tracks"]
                    or [
                        decode(row["system_payload_json"])
                        for row in system_rows
                    ]
                    != payload["systems"]
                    or [decode(row[0]) for row in connection_rows]
                    != payload["connections"]
                    or [decode(row[0]) for row in event_rows]
                    != payload["membership_events"]
                ):
                    raise ValueError("grouping childset wijkt af")

                for system_row, system in zip(
                    system_rows,
                    payload["systems"],
                    strict=True,
                ):
                    system_record_id = str(
                        system_row["weather_system_snapshot_record_id"]
                    )
                    member_rows = db.execute(
                        """
                        SELECT input.track_payload_json
                        FROM meteorological_weather_system_members AS member
                        JOIN meteorological_weather_system_track_inputs AS input
                          USING(weather_system_track_input_id)
                        WHERE member.weather_system_snapshot_record_id=?
                        ORDER BY member.ordinal
                        """,
                        (system_record_id,),
                    ).fetchall()
                    footprint_rows = db.execute(
                        """
                        SELECT pixel_row, pixel_column
                        FROM meteorological_weather_system_footprint_pixels
                        WHERE weather_system_snapshot_record_id=?
                        ORDER BY ordinal
                        """,
                        (system_record_id,),
                    ).fetchall()
                    motion_rows = db.execute(
                        """
                        SELECT track_id
                        FROM meteorological_weather_system_motion_members
                        WHERE weather_system_snapshot_record_id=?
                        ORDER BY ordinal
                        """,
                        (system_record_id,),
                    ).fetchall()
                    predecessor_rows = db.execute(
                        """
                        SELECT predecessor_payload_json
                        FROM
                            meteorological_weather_system_predecessor_candidates
                        WHERE weather_system_snapshot_record_id=?
                        ORDER BY ordinal
                        """,
                        (system_record_id,),
                    ).fetchall()
                    footprint = [
                        [int(row["pixel_row"]), int(row["pixel_column"])]
                        for row in footprint_rows
                    ]
                    footprint_json = _canonical_json(footprint)
                    if (
                        int(system_row["member_count"]) != len(member_rows)
                        or int(system_row["footprint_pixel_count"])
                        != len(footprint_rows)
                        or int(system_row["motion_support_count"])
                        != len(motion_rows)
                        or int(system_row["predecessor_count"])
                        != len(predecessor_rows)
                        or [decode(row[0]) for row in member_rows]
                        != system["member_tracks"]
                        or footprint
                        != system["aggregate_current_footprint"]
                        or hashlib.sha256(
                            footprint_json.encode("utf-8")
                        ).hexdigest()
                        != str(system_row["aggregate_footprint_sha256"])
                        or [str(row["track_id"]) for row in motion_rows]
                        != system["descriptive_motion"][
                            "supporting_track_ids"
                        ]
                        or [decode(row[0]) for row in predecessor_rows]
                        != system["predecessor_candidates"]
                    ):
                        raise ValueError("system childset wijkt af")
            except (
                KeyError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
            ):
                mismatches += 1
        return mismatches

    @staticmethod
    def _site_alias_overlap_count(db: sqlite3.Connection) -> int:
        return int(
            db.execute(
                """
                SELECT count(*)
                FROM meteorological_physical_site_aliases AS left_alias
                JOIN meteorological_physical_site_aliases AS right_alias
                  ON left_alias.site_alias_id < right_alias.site_alias_id
                 AND left_alias.source_id=right_alias.source_id
                 AND left_alias.external_station_id=
                    right_alias.external_station_id
                 AND (
                    left_alias.valid_end IS NULL
                    OR right_alias.valid_start IS NULL
                    OR left_alias.valid_end>right_alias.valid_start
                 )
                 AND (
                    right_alias.valid_end IS NULL
                    OR left_alias.valid_start IS NULL
                    OR right_alias.valid_end>left_alias.valid_start
                 )
                """
            ).fetchone()[0]
        )

    @staticmethod
    def _stats(db: sqlite3.Connection) -> dict[str, int]:
        counts = {
            table.removeprefix("meteorological_"): int(
                db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            )
            for table in _EXPECTED_TABLES
            if table
            not in {
                "meteorological_evidence_metadata",
                "meteorological_evidence_schema_migrations",
            }
        }
        return {
            "schema_version": METEOROLOGICAL_EVIDENCE_SCHEMA_VERSION,
            **counts,
        }

    def _verified_connection(self) -> sqlite3.Connection:
        db = self._connect()
        try:
            row = db.execute(
                """
                SELECT metadata_value
                FROM meteorological_evidence_metadata
                WHERE metadata_key='schema_version'
                """
            ).fetchone()
            if (
                row is None
                or int(row[0]) != METEOROLOGICAL_EVIDENCE_SCHEMA_VERSION
            ):
                raise EvidenceSchemaError(
                    "evidence-store schemaversie wijzigde na initialize()"
                )
            return db
        except BaseException:
            db.close()
            raise

    def _connect(self, *, durable: bool = True) -> sqlite3.Connection:
        db = sqlite3.connect(
            self.path,
            timeout=20,
            isolation_level=None,
        )
        try:
            db.row_factory = sqlite3.Row
            if durable:
                self._configure_durability(db)
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=20000")
            return db
        except BaseException:
            db.close()
            raise

    @staticmethod
    def _configure_durability(db: sqlite3.Connection) -> None:
        journal_mode = str(
            db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        ).lower()
        if journal_mode != "wal":
            db.close()
            raise EvidenceSchemaError(
                f"evidence-store kon WAL niet activeren: {journal_mode}"
            )
        db.execute("PRAGMA synchronous=FULL")

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise EvidenceStoreError(
                "MeteorologicalEvidenceStore moet eerst initialize() uitvoeren"
            )

    @staticmethod
    def _raise_integrity(err: sqlite3.IntegrityError) -> None:
        if "FOREIGN KEY" in str(err).upper():
            raise EvidenceForeignKeyError(
                "evidence verwijst naar een ontbrekend parent-record"
            ) from err
        raise EvidenceStoreError(
            f"evidence-record schendt het immutable schema: {err}"
        ) from err


def _columns_from_definition(definition: str) -> set[str]:
    body = definition.split("(", 1)[1].rsplit(")", 1)[0]
    columns: set[str] = set()
    depth = 0
    segment = ""
    for character in body + ",":
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        if character == "," and depth == 0:
            first = segment.strip().split(None, 1)[0] if segment.strip() else ""
            upper = first.upper()
            if first and not upper.startswith(
                (
                    "CHECK",
                    "CONSTRAINT",
                    "FOREIGN",
                    "PRIMARY",
                    "UNIQUE",
                )
            ):
                columns.add(first)
            segment = ""
        else:
            segment += character
    return columns


def _canonical_json(value: Any) -> str:
    _reject_secret_metadata(value)
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as err:
        raise ValueError("evidence metadata moet canonieke JSON zijn") from err


def _normalize_weather_system_grouping_snapshot(value: Any) -> dict[str, Any]:
    """Convert the immutable grouping dataclass to validated canonical data."""
    if (
        isinstance(value, type)
        or not is_dataclass(value)
        or value.__class__.__name__ != "WeatherSystemGroupingSnapshot"
    ):
        raise TypeError(
            "grouping_snapshot moet een immutable "
            "WeatherSystemGroupingSnapshot zijn"
        )
    raw = asdict(value)
    if not isinstance(raw, Mapping):
        raise TypeError("WeatherSystem grouping kon niet lossless worden gelezen")
    payload = json.loads(_canonical_json(raw))
    _weather_exact_keys(
        payload,
        {
            "grouping_snapshot_id",
            "valid_at",
            "analysis_grid_id",
            "policy",
            "input_tracks",
            "systems",
            "connections",
            "membership_events",
            "previous_grouping_snapshot_id",
            "retired_system_ids",
            "algorithm_version",
        },
        "WeatherSystem grouping",
    )
    payload["grouping_snapshot_id"] = _weather_identifier(
        payload["grouping_snapshot_id"],
        "grouping_snapshot_id",
    )
    payload["valid_at"] = _number(payload["valid_at"], "valid_at")
    payload["analysis_grid_id"] = _weather_identifier(
        payload["analysis_grid_id"],
        "analysis_grid_id",
    )
    payload["algorithm_version"] = _weather_identifier(
        payload["algorithm_version"],
        "algorithm_version",
    )
    previous = payload["previous_grouping_snapshot_id"]
    if previous is not None:
        previous = _weather_identifier(
            previous,
            "previous_grouping_snapshot_id",
        )
    payload["previous_grouping_snapshot_id"] = previous
    payload["retired_system_ids"] = _weather_identifier_list(
        payload["retired_system_ids"],
        "retired_system_ids",
    )

    policy = payload["policy"]
    _weather_exact_keys(
        policy,
        {
            "policy_id",
            "pixel_connectivity",
            "maximum_footprint_gap_m",
            "maximum_motion_difference_m_s",
            "maximum_motion_age_s",
            "maximum_motion_time_difference_s",
            "algorithm_version",
        },
        "WeatherSystem policy",
    )
    policy["policy_id"] = _weather_identifier(
        policy["policy_id"],
        "policy_id",
    )
    if (
        isinstance(policy["pixel_connectivity"], bool)
        or policy["pixel_connectivity"] not in (4, 8)
    ):
        raise ValueError("pixel_connectivity moet 4 of 8 zijn")
    for field in (
        "maximum_footprint_gap_m",
        "maximum_motion_difference_m_s",
        "maximum_motion_age_s",
        "maximum_motion_time_difference_s",
    ):
        policy[field] = _nonnegative_number(policy[field], field)
    policy["algorithm_version"] = _weather_identifier(
        policy["algorithm_version"],
        "policy algorithm_version",
    )
    if policy["algorithm_version"] != payload["algorithm_version"]:
        raise ValueError("policy en grouping algorithm_version verschillen")
    expected_fingerprint = _weather_system_policy_fingerprint(policy)
    supplied_fingerprint = getattr(
        getattr(value, "policy", None),
        "fingerprint",
        None,
    )
    if supplied_fingerprint != expected_fingerprint:
        raise ValueError("WeatherSystem policy fingerprint is niet reproduceerbaar")
    policy["fingerprint"] = expected_fingerprint

    raw_tracks = payload["input_tracks"]
    if not isinstance(raw_tracks, list):
        raise TypeError("input_tracks moet een immutable sequence zijn")
    tracks = [_normalize_weather_system_track(item) for item in raw_tracks]
    if tracks != sorted(tracks, key=lambda item: item["track_id"]):
        raise ValueError("input_tracks moeten canoniek gesorteerd zijn")
    if len({item["track_id"] for item in tracks}) != len(tracks):
        raise ValueError("WeatherSystem input bevat dubbele track_id")
    for track in tracks:
        if (
            track["valid_at"] != payload["valid_at"]
            or track["analysis_grid_id"] != payload["analysis_grid_id"]
        ):
            raise ValueError("WeatherSystem input moet groupingtijd en grid delen")
    payload["input_tracks"] = tracks
    track_by_id = {item["track_id"]: item for item in tracks}

    raw_systems = payload["systems"]
    if not isinstance(raw_systems, list):
        raise TypeError("systems moet een immutable sequence zijn")
    systems = [_normalize_weather_system(item) for item in raw_systems]
    if systems != sorted(systems, key=lambda item: item["system_id"]):
        raise ValueError("WeatherSystems moeten canoniek gesorteerd zijn")
    if len({item["system_id"] for item in systems}) != len(systems):
        raise ValueError("WeatherSystem system_id moet uniek zijn")
    member_track_ids: list[str] = []
    for system in systems:
        if (
            system["valid_at"] != payload["valid_at"]
            or system["analysis_grid_id"] != payload["analysis_grid_id"]
            or system["algorithm_version"] != payload["algorithm_version"]
        ):
            raise ValueError("WeatherSystem snapshot hoort niet bij de grouping")
        for member in system["member_tracks"]:
            track_id = member["track_id"]
            if track_id not in track_by_id or member != track_by_id[track_id]:
                raise ValueError(
                    "WeatherSystem member is niet de exacte inputtrack"
                )
            member_track_ids.append(track_id)
        if set(system["descriptive_motion"]["supporting_track_ids"]) - {
            item["track_id"] for item in system["member_tracks"]
        }:
            raise ValueError("descriptive motion source is geen systemmember")
    if sorted(member_track_ids) != sorted(track_by_id):
        raise ValueError("iedere inputtrack moet exact één systemmember blijven")
    if len(member_track_ids) != len(set(member_track_ids)):
        raise ValueError("track mag niet in meerdere WeatherSystems zitten")
    if set(payload["retired_system_ids"]) & {
        item["system_id"] for item in systems
    }:
        raise ValueError("actueel WeatherSystem kan niet tegelijk retired zijn")
    payload["systems"] = systems

    raw_connections = payload["connections"]
    if not isinstance(raw_connections, list):
        raise TypeError("connections moet een immutable sequence zijn")
    connections = [
        _normalize_weather_system_connection(item)
        for item in raw_connections
    ]
    if connections != sorted(
        connections,
        key=lambda item: (item["left_track_id"], item["right_track_id"]),
    ):
        raise ValueError("WeatherSystem connections zijn niet canoniek gesorteerd")
    expected_pairs = {
        (left, right)
        for index, left in enumerate(sorted(track_by_id))
        for right in sorted(track_by_id)[index + 1 :]
    }
    actual_pairs = {
        (item["left_track_id"], item["right_track_id"])
        for item in connections
    }
    if actual_pairs != expected_pairs:
        raise ValueError("connection evidence dekt niet exact iedere trackpair")
    if len({item["connection_id"] for item in connections}) != len(connections):
        raise ValueError("WeatherSystem connection_id moet uniek zijn")
    payload["connections"] = connections
    for system in systems:
        member_ids = {item["track_id"] for item in system["member_tracks"]}
        expected_uncertain = sorted(
            item["connection_id"]
            for item in connections
            if item["decision"] == "uncertain"
            and member_ids.intersection(
                {
                    item["left_track_id"],
                    item["right_track_id"],
                }
            )
        )
        if system["uncertain_connection_ids"] != expected_uncertain:
            raise ValueError(
                "system uncertain_connection_ids zijn niet de exacte evidence"
            )

    raw_events = payload["membership_events"]
    if not isinstance(raw_events, list):
        raise TypeError("membership_events moet een immutable sequence zijn")
    events = [
        _normalize_weather_system_membership_event(item)
        for item in raw_events
    ]
    if events != sorted(events, key=lambda item: item["current_system_id"]):
        raise ValueError("membership_events zijn niet canoniek gesorteerd")
    system_by_id = {item["system_id"]: item for item in systems}
    if len(events) != len(systems) or {
        item["current_system_id"] for item in events
    } != set(system_by_id):
        raise ValueError("ieder actueel system vereist exact één membership event")
    for event in events:
        system = system_by_id[event["current_system_id"]]
        if event["current_member_track_ids"] != [
            item["track_id"] for item in system["member_tracks"]
        ]:
            raise ValueError("membership eventmembers verschillen van snapshot")
        predecessor_ids = [
            item["previous_system_id"]
            for item in system["predecessor_candidates"]
        ]
        selected_ids = [
            item["previous_system_id"]
            for item in system["predecessor_candidates"]
            if item["selected"]
        ]
        expected_selected = selected_ids[0] if selected_ids else None
        if (
            event["predecessor_system_ids"] != predecessor_ids
            or event["selected_predecessor_system_id"] != expected_selected
        ):
            raise ValueError(
                "membership event bewaart niet exact de predecessorbeslissing"
            )
        expected_lineage = sorted(
            {
                event_id
                for member in system["member_tracks"]
                for event_id in member["lineage_event_ids"]
            }
        )
        expected_identity_alternatives = sorted(
            {
                alternative_id
                for member in system["member_tracks"]
                for alternative_id in member[
                    "identity_alternative_track_ids"
                ]
            }
        )
        if (
            event["member_lineage_event_ids"] != expected_lineage
            or event["member_identity_alternative_track_ids"]
            != expected_identity_alternatives
        ):
            raise ValueError(
                "membership event verliest memberlineage of identityalternatieven"
            )
    payload["membership_events"] = events
    return payload


def _normalize_weather_system_track(value: Any) -> dict[str, Any]:
    _weather_exact_keys(
        value,
        {
            "track_snapshot_id",
            "track_id",
            "analysis_grid_id",
            "valid_at",
            "current_detection_id",
            "object_kind",
            "current_footprint_pixels",
            "rain_shield_detection_ids",
            "x_m",
            "y_m",
            "vx_m_s",
            "vy_m_s",
            "motion_reference_valid_at",
            "motion_method",
            "lifecycle_state",
            "parent_track_ids",
            "child_track_ids",
            "lineage_event_ids",
            "identity_alternative_track_ids",
        },
        "WeatherSystem track input",
    )
    result = dict(value)
    for field in (
        "track_snapshot_id",
        "track_id",
        "analysis_grid_id",
        "current_detection_id",
    ):
        result[field] = _weather_identifier(result[field], field)
    result["valid_at"] = _number(result["valid_at"], "track valid_at")
    if result["object_kind"] not in ("rain_shield", "embedded_core"):
        raise ValueError("ongeldige WeatherSystem object_kind")
    if result["lifecycle_state"] not in (
        "tentative",
        "confirmed",
        "coasting",
    ):
        raise ValueError("ongeldige WeatherSystem lifecycle_state")
    result["current_footprint_pixels"] = _weather_pixels(
        result["current_footprint_pixels"],
        "current_footprint_pixels",
    )
    result["rain_shield_detection_ids"] = _weather_identifier_list(
        result["rain_shield_detection_ids"],
        "rain_shield_detection_ids",
        allow_empty=False,
    )
    if (
        result["object_kind"] == "rain_shield"
        and result["current_detection_id"]
        not in result["rain_shield_detection_ids"]
    ):
        raise ValueError("rain-shieldtrack mist actuele detectie in shield-ID's")
    result["x_m"] = _number(result["x_m"], "track x_m")
    result["y_m"] = _number(result["y_m"], "track y_m")
    vx = _optional_number(result["vx_m_s"], "track vx_m_s")
    vy = _optional_number(result["vy_m_s"], "track vy_m_s")
    reference = _optional_number(
        result["motion_reference_valid_at"],
        "track motion_reference_valid_at",
    )
    method = result["motion_method"]
    if (vx is None) != (vy is None):
        raise ValueError("trackmotion moet beide componenten of geen bevatten")
    if vx is None:
        if reference is not None or method is not None:
            raise ValueError("track zonder motion mag geen motionprovenance hebben")
    else:
        if reference is None or reference > result["valid_at"]:
            raise ValueError("track motionreferentie is afwezig of future")
        method = _weather_identifier(method, "track motion_method")
    result["vx_m_s"] = vx
    result["vy_m_s"] = vy
    result["motion_reference_valid_at"] = reference
    result["motion_method"] = method
    for field in (
        "parent_track_ids",
        "child_track_ids",
        "lineage_event_ids",
        "identity_alternative_track_ids",
    ):
        result[field] = _weather_identifier_list(result[field], field)
    return result


def _normalize_weather_system(value: Any) -> dict[str, Any]:
    _weather_exact_keys(
        value,
        {
            "system_snapshot_id",
            "system_id",
            "valid_at",
            "analysis_grid_id",
            "member_tracks",
            "rain_shield_detection_ids",
            "aggregate_current_footprint",
            "descriptive_motion",
            "classification",
            "system_uncertainty",
            "predecessor_candidates",
            "uncertain_connection_ids",
            "algorithm_version",
        },
        "WeatherSystem snapshot",
    )
    result = dict(value)
    for field in (
        "system_snapshot_id",
        "system_id",
        "analysis_grid_id",
        "algorithm_version",
    ):
        result[field] = _weather_identifier(result[field], field)
    result["valid_at"] = _number(result["valid_at"], "system valid_at")
    if not isinstance(result["member_tracks"], list):
        raise TypeError("system member_tracks moet sequence zijn")
    result["member_tracks"] = [
        _normalize_weather_system_track(item)
        for item in result["member_tracks"]
    ]
    if not result["member_tracks"] or result["member_tracks"] != sorted(
        result["member_tracks"],
        key=lambda item: item["track_id"],
    ):
        raise ValueError("system member_tracks ontbreken of zijn niet gesorteerd")
    result["rain_shield_detection_ids"] = _weather_identifier_list(
        result["rain_shield_detection_ids"],
        "system rain_shield_detection_ids",
        allow_empty=False,
    )
    expected_shields = sorted(
        {
            shield
            for track in result["member_tracks"]
            for shield in track["rain_shield_detection_ids"]
        }
    )
    if result["rain_shield_detection_ids"] != expected_shields:
        raise ValueError("system shield-ID's zijn niet de exacte member-union")
    result["aggregate_current_footprint"] = _weather_pixels(
        result["aggregate_current_footprint"],
        "aggregate_current_footprint",
    )
    expected_footprint = sorted(
        {
            tuple(pixel)
            for track in result["member_tracks"]
            for pixel in track["current_footprint_pixels"]
        }
    )
    if result["aggregate_current_footprint"] != [
        list(pixel) for pixel in expected_footprint
    ]:
        raise ValueError("system footprint is niet de exacte member-union")
    motion = result["descriptive_motion"]
    _weather_exact_keys(
        motion,
        {
            "vx_m_s",
            "vy_m_s",
            "method",
            "supporting_track_ids",
            "maximum_member_residual_m_s",
        },
        "descriptive system motion",
    )
    motion = dict(motion)
    motion["method"] = _weather_identifier(
        motion["method"],
        "descriptive motion method",
    )
    motion["supporting_track_ids"] = _weather_identifier_list(
        motion["supporting_track_ids"],
        "supporting_track_ids",
    )
    motion["vx_m_s"] = _optional_number(
        motion["vx_m_s"],
        "descriptive vx_m_s",
    )
    motion["vy_m_s"] = _optional_number(
        motion["vy_m_s"],
        "descriptive vy_m_s",
    )
    motion["maximum_member_residual_m_s"] = _optional_number(
        motion["maximum_member_residual_m_s"],
        "maximum_member_residual_m_s",
    )
    if (motion["vx_m_s"] is None) != (motion["vy_m_s"] is None):
        raise ValueError("descriptive motionvector is onvolledig")
    if motion["vx_m_s"] is None:
        if (
            motion["supporting_track_ids"]
            or motion["maximum_member_residual_m_s"] is not None
        ):
            raise ValueError("motion zonder vector mag geen support hebben")
    elif (
        not motion["supporting_track_ids"]
        or motion["maximum_member_residual_m_s"] is None
        or motion["maximum_member_residual_m_s"] < 0
    ):
        raise ValueError("descriptive motion mist geldige support/residual")
    result["descriptive_motion"] = motion
    if result["classification"] != "not_classified":
        raise ValueError("evidence-store accepteert geen MCS-classificatie")
    result["system_uncertainty"] = _weather_identifier_list(
        result["system_uncertainty"],
        "system_uncertainty",
    )
    result["uncertain_connection_ids"] = _weather_identifier_list(
        result["uncertain_connection_ids"],
        "uncertain_connection_ids",
    )
    if not isinstance(result["predecessor_candidates"], list):
        raise TypeError("predecessor_candidates moet sequence zijn")
    predecessors = [
        _normalize_weather_system_predecessor(item)
        for item in result["predecessor_candidates"]
    ]
    if predecessors != sorted(
        predecessors,
        key=lambda item: item["previous_system_id"],
    ):
        raise ValueError("predecessor_candidates zijn niet gesorteerd")
    if sum(bool(item["selected"]) for item in predecessors) > 1:
        raise ValueError("WeatherSystem heeft meerdere geselecteerde predecessors")
    result["predecessor_candidates"] = predecessors
    return result


def _normalize_weather_system_predecessor(value: Any) -> dict[str, Any]:
    _weather_exact_keys(
        value,
        {
            "previous_system_id",
            "direct_member_track_ids",
            "lineage_supported_track_ids",
            "selected",
        },
        "WeatherSystem predecessor",
    )
    result = dict(value)
    result["previous_system_id"] = _weather_identifier(
        result["previous_system_id"],
        "previous_system_id",
    )
    result["direct_member_track_ids"] = _weather_identifier_list(
        result["direct_member_track_ids"],
        "direct_member_track_ids",
    )
    result["lineage_supported_track_ids"] = _weather_identifier_list(
        result["lineage_supported_track_ids"],
        "lineage_supported_track_ids",
    )
    if (
        not result["direct_member_track_ids"]
        and not result["lineage_supported_track_ids"]
    ):
        raise ValueError("predecessor vereist aantoonbare tracksupport")
    if not isinstance(result["selected"], bool):
        raise TypeError("predecessor selected moet bool zijn")
    return result


def _normalize_weather_system_connection(value: Any) -> dict[str, Any]:
    _weather_exact_keys(
        value,
        {
            "connection_id",
            "left_track_id",
            "right_track_id",
            "shared_rain_shield_detection_ids",
            "overlap_pixel_count",
            "footprint_gap_m",
            "valid_time_difference_s",
            "motion_time_difference_s",
            "motion_difference_m_s",
            "motion_coherence",
            "spatial_relation",
            "decision",
            "reasons",
        },
        "WeatherSystem connection",
    )
    result = dict(value)
    for field in ("connection_id", "left_track_id", "right_track_id"):
        result[field] = _weather_identifier(result[field], field)
    if result["left_track_id"] >= result["right_track_id"]:
        raise ValueError("connection track-ID's zijn niet canoniek gesorteerd")
    result["shared_rain_shield_detection_ids"] = _weather_identifier_list(
        result["shared_rain_shield_detection_ids"],
        "shared_rain_shield_detection_ids",
    )
    result["overlap_pixel_count"] = _nonnegative_int(
        result["overlap_pixel_count"],
        "overlap_pixel_count",
    )
    result["footprint_gap_m"] = _nonnegative_number(
        result["footprint_gap_m"],
        "footprint_gap_m",
    )
    result["valid_time_difference_s"] = _nonnegative_number(
        result["valid_time_difference_s"],
        "valid_time_difference_s",
    )
    for field in ("motion_time_difference_s", "motion_difference_m_s"):
        number = _optional_number(result[field], field)
        if number is not None and number < 0:
            raise ValueError(f"{field} mag niet negatief zijn")
        result[field] = number
    if result["motion_coherence"] not in (
        "coherent",
        "incoherent",
        "stale",
        "unknown",
    ):
        raise ValueError("ongeldige motion_coherence")
    if result["spatial_relation"] not in (
        "shared_rain_shield",
        "overlap",
        "touching",
        "near",
        "far",
    ):
        raise ValueError("ongeldige spatial_relation")
    if result["decision"] not in ("connected", "uncertain", "separate"):
        raise ValueError("ongeldige WeatherSystem connection decision")
    result["reasons"] = _weather_identifier_list(
        result["reasons"],
        "connection reasons",
        allow_empty=False,
    )
    return result


def _normalize_weather_system_membership_event(value: Any) -> dict[str, Any]:
    _weather_exact_keys(
        value,
        {
            "event_id",
            "event_type",
            "current_system_id",
            "selected_predecessor_system_id",
            "predecessor_system_ids",
            "alternative_predecessor_system_ids",
            "current_member_track_ids",
            "added_track_ids",
            "removed_track_ids",
            "member_lineage_event_ids",
            "member_identity_alternative_track_ids",
        },
        "WeatherSystem membership event",
    )
    result = dict(value)
    result["event_id"] = _weather_identifier(result["event_id"], "event_id")
    result["current_system_id"] = _weather_identifier(
        result["current_system_id"],
        "current_system_id",
    )
    if result["event_type"] not in (
        "birth",
        "continued",
        "membership_changed",
        "merge",
        "split_retained",
        "split_child",
        "complex_reconfiguration",
    ):
        raise ValueError("ongeldig WeatherSystem membership event_type")
    selected = result["selected_predecessor_system_id"]
    if selected is not None:
        selected = _weather_identifier(
            selected,
            "selected_predecessor_system_id",
        )
    result["selected_predecessor_system_id"] = selected
    for field in (
        "predecessor_system_ids",
        "alternative_predecessor_system_ids",
        "current_member_track_ids",
        "added_track_ids",
        "removed_track_ids",
        "member_lineage_event_ids",
        "member_identity_alternative_track_ids",
    ):
        result[field] = _weather_identifier_list(result[field], field)
    if not result["current_member_track_ids"]:
        raise ValueError("membership event vereist actuele membertracks")
    if (
        selected is not None
        and selected not in result["predecessor_system_ids"]
    ):
        raise ValueError("selected predecessor ontbreekt uit event")
    expected_alternatives = [
        item
        for item in result["predecessor_system_ids"]
        if item != selected
    ]
    if result["alternative_predecessor_system_ids"] != expected_alternatives:
        raise ValueError("predecessoralternatieven zijn niet lossless")
    if not set(result["added_track_ids"]) <= set(
        result["current_member_track_ids"]
    ):
        raise ValueError("added tracks zijn geen actuele members")
    if set(result["added_track_ids"]) & set(result["removed_track_ids"]):
        raise ValueError("track is tegelijk added en removed")
    return result


def _weather_exact_keys(
    value: Any,
    expected: set[str],
    name: str,
) -> None:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(
            f"{name} heeft een onbekend of onvolledig evidencecontract"
        )


def _weather_identifier(value: Any, name: str) -> str:
    return _text(value, name, 512)


def _weather_identifier_list(
    value: Any,
    name: str,
    *,
    allow_empty: bool = True,
) -> list[str]:
    if not isinstance(value, list):
        raise TypeError(f"{name} moet een immutable identifiersequence zijn")
    identifiers = [_weather_identifier(item, name) for item in value]
    if not allow_empty and not identifiers:
        raise ValueError(f"{name} mag niet leeg zijn")
    if identifiers != sorted(identifiers) or len(identifiers) != len(
        set(identifiers)
    ):
        raise ValueError(f"{name} moet uniek en gesorteerd zijn")
    return identifiers


def _weather_pixels(value: Any, name: str) -> list[list[int]]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} vereist een niet-lege pixelsequence")
    pixels: list[list[int]] = []
    for pixel in value:
        if (
            not isinstance(pixel, list)
            or len(pixel) != 2
            or isinstance(pixel[0], bool)
            or not isinstance(pixel[0], int)
            or pixel[0] < 0
            or isinstance(pixel[1], bool)
            or not isinstance(pixel[1], int)
            or pixel[1] < 0
        ):
            raise ValueError(f"{name} bevat een ongeldige pixel")
        pixels.append([pixel[0], pixel[1]])
    if pixels != sorted(pixels) or len({tuple(item) for item in pixels}) != len(
        pixels
    ):
        raise ValueError(f"{name} moet uniek en gesorteerd zijn")
    return pixels


def _weather_system_policy_fingerprint(policy: Mapping[str, Any]) -> str:
    parts = (
        str(policy["algorithm_version"]),
        "weather-system-policy",
        str(policy["policy_id"]),
        str(policy["pixel_connectivity"]),
        format(float(policy["maximum_footprint_gap_m"]), ".17g"),
        format(float(policy["maximum_motion_difference_m_s"]), ".17g"),
        format(float(policy["maximum_motion_age_s"]), ".17g"),
        format(float(policy["maximum_motion_time_difference_s"]), ".17g"),
        str(policy["algorithm_version"]),
    )
    digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"weather-system-policy:{digest}"


def _reject_secret_metadata(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            if normalized in _SECRET_JSON_KEYS:
                raise ValueError(
                    f"gevoelig transportveld {key!r} hoort niet in evidence"
                )
            _reject_secret_metadata(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_secret_metadata(child)


def _text(value: str, name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} moet een niet-lege string zijn")
    normalized = value.strip()
    if len(normalized) > maximum:
        raise ValueError(f"{name} is langer dan {maximum} tekens")
    if "\0" in normalized:
        raise ValueError(f"{name} mag geen NUL-teken bevatten")
    return normalized


def _non_url_id(value: str, name: str, maximum: int) -> str:
    normalized = _text(value, name, maximum)
    if "://" in normalized:
        raise ValueError(f"{name} mag geen URL bevatten")
    return normalized


def _record_id(value: str, name: str) -> str:
    return _text(value, name, 256)


def _sha256_hex(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} moet een lowercase SHA-256 zijn")
    return value


def _optional_sha256_hex(value: str | None, name: str) -> str | None:
    return None if value is None else _sha256_hex(value, name)


def _exact_bytes(value: bytes, name: str) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError(f"{name} moet exact bytes zijn")
    return value


def _number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} moet een getal zijn")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} moet eindig zijn")
    return result


def _optional_number(value: float | None, name: str) -> float | None:
    return None if value is None else _number(value, name)


def _positive_number(value: float, name: str) -> float:
    result = _number(value, name)
    if result <= 0:
        raise ValueError(f"{name} moet positief zijn")
    return result


def _nonnegative_number(value: float, name: str) -> float:
    result = _number(value, name)
    if result < 0:
        raise ValueError(f"{name} mag niet negatief zijn")
    return result


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} moet een positief geheel getal zijn")
    return value


def _nonnegative_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} moet een niet-negatief geheel getal zijn")
    return value


def _evidence_interval(
    observed_start: float | None,
    observed_end: float | None,
    received_at: float,
    time_quality: Mapping[str, Any] | None = None,
) -> tuple[float | None, float | None, float, str]:
    """Validate nominal evidence time without rejecting clock anomalies."""
    start = _optional_number(observed_start, "observed_start")
    end = _optional_number(observed_end, "observed_end")
    received = _number(received_at, "received_at")
    if start is None and end is None:
        nominal_state = "unknown"
    elif start is None or end is None:
        nominal_state = "partial"
    else:
        nominal_state = "complete"
    if start is None or end is None:
        interval_order_state = "unknown"
    elif end < start:
        interval_order_state = "inverted"
    else:
        interval_order_state = "ordered"
    quality = {
        "nominal_time_state": nominal_state,
        "interval_order_state": interval_order_state,
        "received_before_observed_start": (
            None if start is None else received < start
        ),
        "received_before_observed_end": (
            None if end is None else received < end
        ),
        "received_minus_observed_start_seconds": (
            None if start is None else received - start
        ),
        "reported": dict(time_quality or {}),
    }
    return start, end, received, _canonical_json(quality)


def _coordinates(
    latitude: float | None,
    longitude: float | None,
    *,
    allow_none: bool,
) -> tuple[float | None, float | None]:
    if latitude is None or longitude is None:
        if allow_none and latitude is None and longitude is None:
            return None, None
        raise ValueError("latitude en longitude moeten samen aanwezig zijn")
    lat = _number(latitude, "latitude")
    lon = _number(longitude, "longitude")
    if lat < -90 or lat > 90:
        raise ValueError("latitude ligt buiten [-90, 90]")
    if lon < -180 or lon > 180:
        raise ValueError("longitude ligt buiten [-180, 180]")
    return lat, lon


def _optional_resolution_pair(
    resolution_x: float | None,
    resolution_y: float | None,
) -> tuple[float | None, float | None]:
    if resolution_x is None or resolution_y is None:
        if resolution_x is None and resolution_y is None:
            return None, None
        raise ValueError("native resoluties moeten samen aanwezig zijn")
    return (
        _positive_number(resolution_x, "native_resolution_x"),
        _positive_number(resolution_y, "native_resolution_y"),
    )
