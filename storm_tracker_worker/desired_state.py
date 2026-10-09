"""Bounded, crash-durable desired state for the worker-only runtime.

Home Assistant is the configuration authority, but it must not schedule
provider work.  It sends this small contract only when targets or relevant
settings change.  The worker persists it with compare-and-swap semantics and
uses the active record to schedule all meteorological work itself.

The contract deliberately contains no credentials.  Provider secrets remain
in the add-on environment and can therefore never enter MQTT job payloads,
receipts or diagnostics.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Mapping
import unicodedata


CONTRACT = "stv3-worker-desired-state-v1"
METHOD = "worker_desired_state_store_v1"
SCHEMA_VERSION = 3
LEGACY_SCHEMA_VERSION = 2
STORE_IDENTITY_SCHEMA = "stv3-worker-desired-state-store-identity-v1"
MAX_CONTRACT_BYTES = 64 * 1024
MAX_TARGETS = 64
MAX_DATABASE_BYTES = 256 * 1024 * 1024
MAX_STATE_ROWS = 250_000
MAX_AUTHORITY_CLAIMS = 100_000
MAX_CLOCK_FUTURE_SKEW_SECONDS = 5 * 60
MAX_DYNAMIC_POSITION_AGE_SECONDS = 15 * 60
DEFAULT_STORE_PATH = Path(
    os.getenv(
        "STV3_DESIRED_STATE_DB",
        "/share/storm_tracker_worker/desired_state.sqlite3",
    )
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
_COUNTRY = re.compile(r"^[A-Z]{2}$")


class DesiredStateError(ValueError):
    """The desired-state contract or state transition is invalid."""


class DesiredStateConflict(DesiredStateError):
    """The desired state does not extend the active compare-and-swap head."""


@dataclass(frozen=True)
class Target:
    target_id: str
    display_name: str
    kind: str
    available: bool
    latitude: float
    longitude: float
    position_observed_at: float | None
    location_accuracy_m: float | None
    observation_radius_km: float
    country_code: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "country_code": self.country_code,
            "display_name": self.display_name,
            "kind": self.kind,
            "latitude": self.latitude,
            "location_accuracy_m": self.location_accuracy_m,
            "longitude": self.longitude,
            "observation_radius_km": self.observation_radius_km,
            "position_observed_at": self.position_observed_at,
            "target_id": self.target_id,
        }


@dataclass(frozen=True)
class RuntimeSettings:
    ground_radius_km: float
    lightning_source_mode: str
    model_guidance_enabled: bool
    region_sharing_distance_km: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "ground_radius_km": self.ground_radius_km,
            "lightning_source_mode": self.lightning_source_mode,
            "model_guidance_enabled": self.model_guidance_enabled,
            "region_sharing_distance_km": self.region_sharing_distance_km,
        }


@dataclass(frozen=True)
class DesiredState:
    producer_id: str
    authority_epoch: int
    generation: int
    issued_at: float
    previous_state_sha256: str | None
    targets: tuple[Target, ...]
    settings: RuntimeSettings
    canonical_bytes: bytes
    state_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return json.loads(self.canonical_bytes)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DesiredStateError(f"dubbele JSON-sleutel: {key}")
        result[key] = value
    return result


def _decode(value: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, Mapping):
        try:
            encoded = json.dumps(
                dict(value),
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as err:
            raise DesiredStateError("desired-state is niet canoniek JSON") from err
        if len(encoded) > MAX_CONTRACT_BYTES:
            raise DesiredStateError("desired-state overschrijdt het bytebudget")
        decoded = dict(value)
    else:
        if isinstance(value, str):
            try:
                encoded = value.encode("utf-8")
            except UnicodeEncodeError as err:
                raise DesiredStateError("desired-state is geen geldige UTF-8") from err
        elif isinstance(value, bytes):
            encoded = value
        else:
            raise DesiredStateError("desired-state vereist bytes, tekst of object")
        if len(encoded) > MAX_CONTRACT_BYTES:
            raise DesiredStateError("desired-state overschrijdt het bytebudget")
        try:
            decoded = json.loads(
                encoded.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=lambda token: (_ for _ in ()).throw(
                    DesiredStateError(f"niet-eindig JSON-getal: {token}")
                ),
            )
        except DesiredStateError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise DesiredStateError("desired-state bevat ongeldige JSON") from err
    if not isinstance(decoded, dict):
        raise DesiredStateError("desired-state moet een JSON-object zijn")
    return decoded


def _exact_keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise DesiredStateError(
            f"{name} heeft ongeldige sleutels; ontbrekend={missing}, extra={extra}"
        )


def _identifier(value: Any, name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str):
        raise DesiredStateError(f"{name} moet tekst zijn")
    result = value.strip()
    if not result or len(result) > maximum or not _IDENTIFIER.fullmatch(result):
        raise DesiredStateError(f"{name} is geen geldig begrensd ID")
    return result


def _display_text(value: Any, name: str, *, maximum: int = 128) -> str:
    if not isinstance(value, str):
        raise DesiredStateError(f"{name} moet tekst zijn")
    if value != value.strip() or not value or len(value) > maximum:
        raise DesiredStateError(f"{name} is geen begrensde tekst")
    if value != unicodedata.normalize("NFC", value):
        raise DesiredStateError(f"{name} moet NFC-genormaliseerd zijn")
    if any(ord(character) < 32 for character in value):
        raise DesiredStateError(f"{name} bevat controletekens")
    return value


def _number(
    value: Any,
    name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DesiredStateError(f"{name} moet een getal zijn")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise DesiredStateError(
            f"{name} moet eindig en binnen [{minimum}, {maximum}] zijn"
        )
    return result


def _parse_target(value: Any) -> Target:
    if not isinstance(value, Mapping):
        raise DesiredStateError("ieder target moet een object zijn")
    _exact_keys(
        value,
        {
            "target_id",
            "display_name",
            "kind",
            "available",
            "latitude",
            "longitude",
            "position_observed_at",
            "location_accuracy_m",
            "observation_radius_km",
            "country_code",
        },
        "target",
    )
    kind = str(value["kind"])
    if kind not in {"home", "person", "test", "fixed", "device_tracker"}:
        raise DesiredStateError("target.kind is niet ondersteund")
    available = value["available"]
    if type(available) is not bool:
        raise DesiredStateError("target.available moet boolean zijn")
    position_observed_at = value["position_observed_at"]
    if position_observed_at is not None:
        position_observed_at = _number(
            position_observed_at,
            "position_observed_at",
            minimum=0.0,
            maximum=2**53,
        )
    if available and position_observed_at is None:
        raise DesiredStateError(
            "een beschikbaar target vereist position_observed_at"
        )
    location_accuracy_m = value["location_accuracy_m"]
    if location_accuracy_m is not None:
        location_accuracy_m = _number(
            location_accuracy_m,
            "location_accuracy_m",
            minimum=0.0,
            maximum=100_000.0,
        )
    country = value["country_code"]
    if country is not None:
        if not isinstance(country, str) or not _COUNTRY.fullmatch(country):
            raise DesiredStateError("country_code moet null of ISO alpha-2 zijn")
    return Target(
        target_id=_identifier(value["target_id"], "target_id"),
        display_name=_display_text(value["display_name"], "display_name"),
        kind=kind,
        available=available,
        latitude=_number(
            value["latitude"], "latitude", minimum=-90.0, maximum=90.0
        ),
        longitude=_number(
            value["longitude"], "longitude", minimum=-180.0, maximum=180.0
        ),
        position_observed_at=position_observed_at,
        location_accuracy_m=location_accuracy_m,
        observation_radius_km=_number(
            value["observation_radius_km"],
            "observation_radius_km",
            minimum=1.0,
            maximum=1000.0,
        ),
        country_code=country,
    )


def _parse_settings(value: Any) -> RuntimeSettings:
    if not isinstance(value, Mapping):
        raise DesiredStateError("settings moet een object zijn")
    _exact_keys(
        value,
        {
            "ground_radius_km",
            "lightning_source_mode",
            "model_guidance_enabled",
            "region_sharing_distance_km",
        },
        "settings",
    )
    mode = value["lightning_source_mode"]
    if mode not in {"auto", "satellite_test"}:
        raise DesiredStateError("lightning_source_mode is niet ondersteund")
    enabled = value["model_guidance_enabled"]
    if type(enabled) is not bool:
        raise DesiredStateError("model_guidance_enabled moet boolean zijn")
    return RuntimeSettings(
        ground_radius_km=_number(
            value["ground_radius_km"],
            "ground_radius_km",
            minimum=1.0,
            maximum=100.0,
        ),
        lightning_source_mode=str(mode),
        model_guidance_enabled=enabled,
        region_sharing_distance_km=_number(
            value["region_sharing_distance_km"],
            "region_sharing_distance_km",
            minimum=1.0,
            maximum=500.0,
        ),
    )


def parse_desired_state(value: bytes | str | Mapping[str, Any]) -> DesiredState:
    """Validate and canonicalise one complete desired-state document."""

    decoded = _decode(value)
    _exact_keys(
        decoded,
        {
            "contract",
            "producer_id",
            "authority_epoch",
            "generation",
            "issued_at",
            "previous_state_sha256",
            "targets",
            "settings",
        },
        "desired-state",
    )
    if decoded["contract"] != CONTRACT:
        raise DesiredStateError("desired-state contractversie is niet ondersteund")
    producer_id = _identifier(decoded["producer_id"], "producer_id")
    authority_epoch = decoded["authority_epoch"]
    if (
        isinstance(authority_epoch, bool)
        or not isinstance(authority_epoch, int)
        or not 1 <= authority_epoch <= 2**63 - 1
    ):
        raise DesiredStateError(
            "authority_epoch moet een positieve 64-bit integer zijn"
        )
    generation = decoded["generation"]
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or not 0 <= generation <= 2**63 - 1
    ):
        raise DesiredStateError("generation moet een niet-negatieve 64-bit integer zijn")
    issued_at = _number(
        decoded["issued_at"], "issued_at", minimum=0.0, maximum=2**53
    )
    previous = decoded["previous_state_sha256"]
    if previous is not None and (
        not isinstance(previous, str) or not _SHA256.fullmatch(previous)
    ):
        raise DesiredStateError("previous_state_sha256 is ongeldig")
    raw_targets = decoded["targets"]
    if not isinstance(raw_targets, list) or len(raw_targets) > MAX_TARGETS:
        raise DesiredStateError("targets moet een begrensde lijst zijn")
    targets = tuple(sorted((_parse_target(item) for item in raw_targets), key=lambda t: t.target_id))
    target_ids = [item.target_id for item in targets]
    if len(set(target_ids)) != len(target_ids):
        raise DesiredStateError("target_id moet uniek zijn")
    settings = _parse_settings(decoded["settings"])
    canonical_object = {
        "authority_epoch": authority_epoch,
        "contract": CONTRACT,
        "generation": generation,
        "issued_at": issued_at,
        "previous_state_sha256": previous,
        "producer_id": producer_id,
        "settings": settings.as_dict(),
        "targets": [item.as_dict() for item in targets],
    }
    canonical = json.dumps(
        canonical_object,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(canonical) > MAX_CONTRACT_BYTES:
        raise DesiredStateError("canonieke desired-state overschrijdt het bytebudget")
    digest = hashlib.sha256(canonical).hexdigest()
    return DesiredState(
        producer_id=producer_id,
        authority_epoch=authority_epoch,
        generation=generation,
        issued_at=issued_at,
        previous_state_sha256=previous,
        targets=targets,
        settings=settings,
        canonical_bytes=canonical,
        state_sha256=digest,
    )


class DesiredStateStore:
    """SQLite CAS head plus immutable canonical configuration versions."""

    _EXPECTED_TABLES = frozenset({
        "schema_meta",
        "producer_authority",
        "authority_claims",
        "desired_states",
        "active_desired_state",
    })
    _EXPECTED_COLUMNS = {
        "schema_meta": frozenset({
            "singleton", "schema_version", "store_instance_sha256",
        }),
        "producer_authority": frozenset({
            "singleton", "producer_id", "authority_epoch", "claim_id",
            "claimed_at",
        }),
        "authority_claims": frozenset({
            "claim_id", "producer_id", "authority_epoch", "claimed_at",
        }),
        "desired_states": frozenset({
            "state_sha256", "canonical_json", "producer_id",
            "authority_epoch", "generation", "issued_at",
            "previous_state_sha256", "target_count", "first_received_at",
            "last_received_at", "receipt_count",
        }),
        "active_desired_state": frozenset({
            "singleton", "state_sha256", "activated_at",
        }),
    }

    def __init__(
        self,
        path: str | Path = DEFAULT_STORE_PATH,
        *,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        self._fault_injector = fault_injector
        self._store_instance_sha256: str | None = None

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        existed = self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=30.0)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA wal_autocheckpoint=128")
            db.execute("PRAGMA journal_size_limit=8388608")
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(1, MAX_DATABASE_BYTES // page_size)
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
            if page_count > maximum_pages:
                raise DesiredStateError(
                    "desired-state database overschrijdt de bytegrens"
                )
            db.execute(f"PRAGMA max_page_count={maximum_pages}")
            if self._store_instance_sha256 is None:
                current_instance = self._ensure_schema(db)
            else:
                # The long-lived worker reads desired-state on every route
                # boundary.  Schema DDL and BEGIN IMMEDIATE belong only to
                # first-open/migration, never to that read hot path.
                current_instance = self._load_store_instance(db)
            self._validate_integrity(db, full=not existed)
            if self._store_instance_sha256 is None:
                self._store_instance_sha256 = current_instance
            elif not hmac.compare_digest(
                current_instance, self._store_instance_sha256
            ):
                raise DesiredStateError(
                    "desired-state store-instance authority is gewijzigd"
                )
            if not existed and os.name == "posix":
                directory_fd = os.open(
                    self.path.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            return db
        except BaseException:
            db.close()
            raise

    @staticmethod
    def _normalized_sql(statement: str) -> str:
        return " ".join(statement.replace(";", "").split()).lower()

    @classmethod
    def _ensure_schema(cls, db: sqlite3.Connection) -> str:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version INTEGER NOT NULL,
                store_instance_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS producer_authority (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                producer_id TEXT NOT NULL,
                authority_epoch INTEGER NOT NULL CHECK(authority_epoch >= 1),
                claim_id TEXT NOT NULL UNIQUE,
                claimed_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS authority_claims (
                claim_id TEXT PRIMARY KEY,
                producer_id TEXT NOT NULL,
                authority_epoch INTEGER NOT NULL CHECK(authority_epoch >= 1),
                claimed_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS desired_states (
                state_sha256 TEXT PRIMARY KEY,
                canonical_json BLOB NOT NULL,
                producer_id TEXT NOT NULL,
                authority_epoch INTEGER NOT NULL CHECK(authority_epoch >= 1),
                generation INTEGER NOT NULL,
                issued_at REAL NOT NULL,
                previous_state_sha256 TEXT,
                target_count INTEGER NOT NULL,
                first_received_at REAL NOT NULL,
                last_received_at REAL NOT NULL,
                receipt_count INTEGER NOT NULL CHECK(receipt_count >= 1)
            );
            CREATE TABLE IF NOT EXISTS active_desired_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                state_sha256 TEXT NOT NULL REFERENCES desired_states(state_sha256),
                activated_at REAL NOT NULL
            );
            """
        )
        db.execute("BEGIN IMMEDIATE")
        columns = {
            str(row[1]) for row in db.execute("PRAGMA table_info(schema_meta)")
        }
        legacy_columns = {"singleton", "schema_version"}
        current_columns = legacy_columns | {"store_instance_sha256"}
        if columns == legacy_columns:
            db.execute(
                "ALTER TABLE schema_meta ADD COLUMN store_instance_sha256 TEXT"
            )
            columns = current_columns
        if columns != current_columns:
            raise DesiredStateError(
                "desired-state schemamanifest heeft onverwachte kolommen"
            )
        rows = db.execute(
            "SELECT singleton,schema_version,store_instance_sha256 "
            "FROM schema_meta"
        ).fetchall()
        if len(rows) > 1:
            raise DesiredStateError("desired-state schemamanifest is corrupt")
        row = rows[0] if rows else None
        installing_authority = row is None
        if row is None:
            instance = hashlib.sha256(os.urandom(32)).hexdigest()
            db.execute(
                "INSERT INTO schema_meta("
                "singleton,schema_version,store_instance_sha256) "
                "VALUES (1, ?, ?)",
                (SCHEMA_VERSION, instance),
            )
        else:
            if int(row["singleton"]) != 1:
                raise DesiredStateError("desired-state schemamanifest is corrupt")
            version = int(row["schema_version"])
            instance_value = row["store_instance_sha256"]
            if version == LEGACY_SCHEMA_VERSION:
                installing_authority = True
                if instance_value is None:
                    instance_value = hashlib.sha256(os.urandom(32)).hexdigest()
                    db.execute(
                        "UPDATE schema_meta SET store_instance_sha256=? "
                        "WHERE singleton=1 AND schema_version=? "
                        "AND store_instance_sha256 IS NULL",
                        (instance_value, LEGACY_SCHEMA_VERSION),
                    )
                db.execute(
                    "UPDATE schema_meta SET schema_version=? "
                    "WHERE singleton=1 AND schema_version=?",
                    (SCHEMA_VERSION, LEGACY_SCHEMA_VERSION),
                )
            elif version != SCHEMA_VERSION:
                raise DesiredStateError(
                    "desired-state databaseversie is niet ondersteund"
                )
        trigger_sql = {
            "schema_meta_no_update": (
                "CREATE TRIGGER schema_meta_no_update BEFORE UPDATE ON "
                "schema_meta BEGIN SELECT RAISE(ABORT, "
                "'desired-state schema authority is immutable'); END"
            ),
            "schema_meta_no_delete": (
                "CREATE TRIGGER schema_meta_no_delete BEFORE DELETE ON "
                "schema_meta BEGIN SELECT RAISE(ABORT, "
                "'desired-state schema authority is immutable'); END"
            ),
        }
        for name, statement in trigger_sql.items():
            if db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?",
                (name,),
            ).fetchone() is None:
                if not installing_authority:
                    raise DesiredStateError(
                        "desired-state schema-authoritytrigger ontbreekt"
                    )
                db.execute(statement)
        actual_triggers = {
            str(row["name"]): str(row["sql"])
            for row in db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        if set(actual_triggers) != set(trigger_sql) or any(
            cls._normalized_sql(actual_triggers[name])
            != cls._normalized_sql(statement)
            for name, statement in trigger_sql.items()
        ):
            raise DesiredStateError(
                "desired-state schema-authoritytriggers zijn gewijzigd"
            )
        db.commit()
        return cls._load_store_instance(db)

    @staticmethod
    def _load_store_instance(db: sqlite3.Connection) -> str:
        rows = db.execute(
            "SELECT singleton,schema_version,store_instance_sha256 "
            "FROM schema_meta"
        ).fetchall()
        if len(rows) != 1:
            raise DesiredStateError("desired-state schemamanifest is corrupt")
        row = rows[0]
        instance = str(row["store_instance_sha256"])
        if (
            int(row["singleton"]) != 1
            or int(row["schema_version"]) != SCHEMA_VERSION
            or _SHA256.fullmatch(instance) is None
        ):
            raise DesiredStateError(
                "desired-state store-instance authority is ongeldig"
            )
        return instance

    @property
    def store_instance_sha256(self) -> str:
        if self._store_instance_sha256 is None:
            with closing(self._connect()):
                pass
        instance = self._store_instance_sha256
        if instance is None:  # pragma: no cover - defensive invariant
            raise DesiredStateError(
                "desired-state store-instance authority ontbreekt"
            )
        with closing(self._connect()) as db:
            current = self._load_store_instance(db)
        if not hmac.compare_digest(instance, current):
            raise DesiredStateError(
                "desired-state store-instance authority is gewijzigd"
            )
        return instance

    @property
    def identity_sha256(self) -> str:
        instance = self.store_instance_sha256
        path = os.path.normcase(
            os.path.normpath(str(self.path.resolve(strict=True)))
        )
        payload = {
            "contract": CONTRACT,
            "limits": {
                "max_authority_claims": MAX_AUTHORITY_CLAIMS,
                "max_clock_future_skew_seconds": MAX_CLOCK_FUTURE_SKEW_SECONDS,
                "max_contract_bytes": MAX_CONTRACT_BYTES,
                "max_database_bytes": MAX_DATABASE_BYTES,
                "max_dynamic_position_age_seconds": (
                    MAX_DYNAMIC_POSITION_AGE_SECONDS
                ),
                "max_state_rows": MAX_STATE_ROWS,
                "max_targets": MAX_TARGETS,
            },
            "method": METHOD,
            "path_sha256": hashlib.sha256(path.encode("utf-8")).hexdigest(),
            "schema": STORE_IDENTITY_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "store_instance_sha256": instance,
        }
        return hashlib.sha256(json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")).hexdigest()

    @classmethod
    def _validate_integrity(
        cls, db: sqlite3.Connection, *, full: bool = False,
    ) -> None:
        tables = {
            str(row[0])
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        if tables != cls._EXPECTED_TABLES:
            raise DesiredStateError(
                f"desired-state schema heeft onverwachte tabellen: {sorted(tables)}"
            )
        for table, expected in cls._EXPECTED_COLUMNS.items():
            columns = {
                str(row[1])
                for row in db.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if columns != expected:
                raise DesiredStateError(
                    f"desired-state tabel {table} heeft onverwachte kolommen"
                )
        if full:
            if db.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                raise DesiredStateError("desired-state database faalt quick_check")
            if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise DesiredStateError("desired-state database faalt foreign_key_check")

    def audit(self) -> dict[str, Any]:
        """Run the expensive full audit explicitly, never on the update hot path."""

        with closing(self._connect()) as db, db:
            self._validate_integrity(db, full=True)
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
            state_rows = int(
                db.execute("SELECT count(*) FROM desired_states").fetchone()[0]
            )
            claim_rows = int(
                db.execute("SELECT count(*) FROM authority_claims").fetchone()[0]
            )
        return {
            "method": METHOD,
            "status": "ok",
            "database_bytes": page_size * page_count,
            "database_limit_bytes": MAX_DATABASE_BYTES,
            "state_rows": state_rows,
            "state_row_limit": MAX_STATE_ROWS,
            "authority_claim_rows": claim_rows,
            "authority_claim_limit": MAX_AUTHORITY_CLAIMS,
        }

    @staticmethod
    def _load_authority(db: sqlite3.Connection) -> sqlite3.Row | None:
        authority = db.execute(
            "SELECT producer_id,authority_epoch,claim_id,claimed_at "
            "FROM producer_authority WHERE singleton=1"
        ).fetchone()
        if authority is None:
            return None
        claim = db.execute(
            "SELECT producer_id,authority_epoch,claimed_at FROM authority_claims "
            "WHERE claim_id=?",
            (authority["claim_id"],),
        ).fetchone()
        if claim is None or (
            str(claim["producer_id"]) != str(authority["producer_id"])
            or int(claim["authority_epoch"]) != int(authority["authority_epoch"])
            or float(claim["claimed_at"]) != float(authority["claimed_at"])
        ):
            raise DesiredStateError("desired-state authorityhead is corrupt")
        return authority

    @staticmethod
    def _state_from_row(row: sqlite3.Row) -> DesiredState:
        try:
            state = parse_desired_state(bytes(row["canonical_json"]))
        except (KeyError, TypeError, ValueError) as err:
            raise DesiredStateError(
                "desired-state canonical record is corrupt"
            ) from err
        expected = {
            "state_sha256": state.state_sha256,
            "producer_id": state.producer_id,
            "authority_epoch": state.authority_epoch,
            "generation": state.generation,
            "issued_at": state.issued_at,
            "previous_state_sha256": state.previous_state_sha256,
            "target_count": len(state.targets),
        }
        for name, value in expected.items():
            if row[name] != value:
                raise DesiredStateError(
                    f"desired-state metadata {name} wijkt af van canonical record"
                )
        return state

    @classmethod
    def _load_active(
        cls, db: sqlite3.Connection,
    ) -> tuple[DesiredState, sqlite3.Row] | None:
        head = db.execute(
            "SELECT state_sha256,activated_at FROM active_desired_state "
            "WHERE singleton=1"
        ).fetchone()
        if head is None:
            return None
        row = db.execute(
            "SELECT * FROM desired_states WHERE state_sha256=?",
            (head["state_sha256"],),
        ).fetchone()
        if row is None:
            raise DesiredStateError("desired-state active head is verweesd")
        state = cls._state_from_row(row)
        if state.state_sha256 != str(head["state_sha256"]):
            raise DesiredStateError("desired-state active head checksum faalt")
        return state, row

    def claim_producer(
        self,
        producer_id: str,
        claim_id: str,
        *,
        claimed_at: float | None = None,
    ) -> dict[str, Any]:
        """Issue a durable fencing epoch to exactly one HA producer boot."""

        producer = _identifier(producer_id, "producer_id")
        claim = _identifier(claim_id, "claim_id")
        claimed = _number(
            time.time() if claimed_at is None else claimed_at,
            "claimed_at",
            minimum=0.0,
            maximum=2**53,
        )
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            previous_claim = db.execute(
                "SELECT producer_id,authority_epoch,claimed_at "
                "FROM authority_claims WHERE claim_id=?",
                (claim,),
            ).fetchone()
            if previous_claim is not None:
                if str(previous_claim["producer_id"]) != producer:
                    raise DesiredStateConflict(
                        "claim_id werd voor een andere producer hergebruikt"
                    )
                db.commit()
                return {
                    "method": METHOD,
                    "status": "claim_idempotent",
                    "producer_id": producer,
                    "authority_epoch": int(previous_claim["authority_epoch"]),
                    "claim_id": claim,
                    "claimed_at": float(previous_claim["claimed_at"]),
                }
            if int(
                db.execute("SELECT count(*) FROM authority_claims").fetchone()[0]
            ) >= MAX_AUTHORITY_CLAIMS:
                raise DesiredStateError("desired-state authorityclaimgrens bereikt")
            authority = self._load_authority(db)
            if authority is None:
                epoch = 1
            else:
                if claimed < float(authority["claimed_at"]):
                    raise DesiredStateConflict("authority claimtijd mag niet regressief zijn")
                epoch = int(authority["authority_epoch"]) + 1
                if epoch > 2**63 - 1:
                    raise DesiredStateError("authority_epoch is uitgeput")
            db.execute(
                "INSERT INTO authority_claims("
                "claim_id,producer_id,authority_epoch,claimed_at) VALUES(?,?,?,?)",
                (claim, producer, epoch, claimed),
            )
            self._fault("claim.after_receipt")
            db.execute(
                "INSERT INTO producer_authority("
                "singleton,producer_id,authority_epoch,claim_id,claimed_at) "
                "VALUES(1,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                "producer_id=excluded.producer_id,"
                "authority_epoch=excluded.authority_epoch,"
                "claim_id=excluded.claim_id,claimed_at=excluded.claimed_at",
                (producer, epoch, claim, claimed),
            )
            self._fault("claim.after_head")
            self._fault("claim.before_commit")
            db.commit()
            self._fault("claim.after_commit")
        return {
            "method": METHOD,
            "status": "claimed",
            "producer_id": producer,
            "authority_epoch": epoch,
            "claim_id": claim,
            "claimed_at": claimed,
        }

    @staticmethod
    def _validate_state_times(
        state: DesiredState,
        *,
        received_at: float,
        active: DesiredState | None,
    ) -> None:
        if state.issued_at > received_at + MAX_CLOCK_FUTURE_SKEW_SECONDS:
            raise DesiredStateConflict("desired-state issued_at ligt onplausibel in de toekomst")
        dynamic_kinds = {"person", "device_tracker", "test"}
        for target in state.targets:
            observed = target.position_observed_at
            if target.available and target.kind in dynamic_kinds:
                if observed is None:  # guarded by the parser, retained defensively
                    raise DesiredStateConflict("bewegend target mist position_observed_at")
                if observed > received_at + MAX_CLOCK_FUTURE_SKEW_SECONDS:
                    raise DesiredStateConflict("targetpositie ligt onplausibel in de toekomst")
                if received_at - observed > MAX_DYNAMIC_POSITION_AGE_SECONDS:
                    raise DesiredStateConflict("beschikbare targetpositie is te oud")
        if active is None:
            return
        if (
            state.producer_id == active.producer_id
            and state.authority_epoch == active.authority_epoch
            and state.issued_at < active.issued_at
        ):
            raise DesiredStateConflict("issued_at mag voor dezelfde producer niet regressief zijn")
        previous_targets = {target.target_id: target for target in active.targets}
        for target in state.targets:
            previous = previous_targets.get(target.target_id)
            if previous is None:
                continue
            if (
                previous.position_observed_at is not None
                and target.position_observed_at is not None
                and target.position_observed_at < previous.position_observed_at
            ):
                raise DesiredStateConflict(
                    f"position_observed_at van {target.target_id} is regressief"
                )

    def apply(
        self,
        value: bytes | str | Mapping[str, Any],
        *,
        received_at: float | None = None,
    ) -> dict[str, Any]:
        state = parse_desired_state(value)
        received = _number(
            time.time() if received_at is None else received_at,
            "received_at",
            minimum=0.0,
            maximum=2**53,
        )
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            authority = self._load_authority(db)
            if authority is None:
                raise DesiredStateConflict("geen actieve desired-state producer authority")
            if (
                state.producer_id != str(authority["producer_id"])
                or state.authority_epoch != int(authority["authority_epoch"])
            ):
                raise DesiredStateConflict("desired-state producer is door fencing afgewezen")
            active_record = self._load_active(db)
            active_state = active_record[0] if active_record is not None else None
            self._validate_state_times(
                state,
                received_at=received,
                active=active_state,
            )
            if active_state is not None and active_state.state_sha256 == state.state_sha256:
                db.execute(
                    "UPDATE desired_states SET last_received_at=max(last_received_at, ?), "
                    "receipt_count=receipt_count+1 WHERE state_sha256=?",
                    (received, state.state_sha256),
                )
                receipt = db.execute(
                    "SELECT first_received_at,last_received_at,receipt_count "
                    "FROM desired_states WHERE state_sha256=?",
                    (state.state_sha256,),
                ).fetchone()
                db.commit()
                return self._result(
                    state,
                    status="idempotent",
                    first_received_at=float(receipt["first_received_at"]),
                    last_received_at=float(receipt["last_received_at"]),
                    receipt_count=int(receipt["receipt_count"]),
                )

            if active_state is None:
                if state.previous_state_sha256 is not None:
                    raise DesiredStateConflict(
                        "eerste desired-state moet previous_state_sha256=null hebben"
                    )
            else:
                active_sha = active_state.state_sha256
                if state.previous_state_sha256 != active_sha:
                    raise DesiredStateConflict(
                        "desired-state bouwt niet op de actieve SHA-256 voort"
                    )
                if (
                    state.producer_id == active_state.producer_id
                    and state.authority_epoch == active_state.authority_epoch
                ):
                    active_generation = active_state.generation
                    if state.generation <= active_generation:
                        raise DesiredStateConflict(
                            "generation moet voor dezelfde producer strikt stijgen"
                        )

            existing = db.execute(
                "SELECT * FROM desired_states WHERE state_sha256=?",
                (state.state_sha256,),
            ).fetchone()
            if existing is None:
                if int(
                    db.execute("SELECT count(*) FROM desired_states").fetchone()[0]
                ) >= MAX_STATE_ROWS:
                    raise DesiredStateError("desired-state historiekgrens bereikt")
                db.execute(
                    "INSERT INTO desired_states("
                    "state_sha256, canonical_json, producer_id, authority_epoch, "
                    "generation, issued_at, "
                    "previous_state_sha256, target_count, first_received_at, "
                    "last_received_at, receipt_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (
                        state.state_sha256,
                        state.canonical_bytes,
                        state.producer_id,
                        state.authority_epoch,
                        state.generation,
                        state.issued_at,
                        state.previous_state_sha256,
                        len(state.targets),
                        received,
                        received,
                    ),
                )
                self._fault("apply.after_state")
            else:
                existing_state = self._state_from_row(existing)
                if existing_state.canonical_bytes != state.canonical_bytes:
                    raise DesiredStateConflict("SHA-256 collision in desired-state store")
            db.execute(
                "INSERT INTO active_desired_state(singleton, state_sha256, activated_at) "
                "VALUES (1, ?, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET "
                "state_sha256=excluded.state_sha256, activated_at=excluded.activated_at",
                (state.state_sha256, received),
            )
            self._fault("apply.after_head")
            self._fault("apply.before_commit")
            db.commit()
            self._fault("apply.after_commit")
        return self._result(
            state,
            status="activated",
            first_received_at=received,
            last_received_at=received,
            receipt_count=1,
        )

    @staticmethod
    def _result(
        state: DesiredState,
        *,
        status: str,
        first_received_at: float,
        last_received_at: float,
        receipt_count: int,
    ) -> dict[str, Any]:
        return {
            "method": METHOD,
            "status": status,
            "state_sha256": state.state_sha256,
            "producer_id": state.producer_id,
            "authority_epoch": state.authority_epoch,
            "generation": state.generation,
            "issued_at": state.issued_at,
            "first_received_at": first_received_at,
            "last_received_at": last_received_at,
            "receipt_count": receipt_count,
            "target_count": len(state.targets),
        }

    def load_active(self) -> DesiredState | None:
        """Return the full state only to in-process worker scheduling code."""

        with closing(self._connect()) as db, db:
            active = self._load_active(db)
        return None if active is None else active[0]

    def load_version(self, state_sha256: str) -> DesiredState | None:
        """Return one immutable historical state by its exact content hash.

        Acquisition completions are allowed to outlive the active desired-state
        head.  A worker-side lifecycle bridge therefore needs the original
        authority document when replaying the crash window between a terminal
        acquisition receipt and completion publication.  This lookup never
        falls back to the active head and reuses the same checksum/metadata
        validation as :meth:`load_active`.
        """

        if (
            not isinstance(state_sha256, str)
            or _SHA256.fullmatch(state_sha256) is None
        ):
            raise DesiredStateError("state_sha256 is ongeldig")
        state_hash = state_sha256
        with closing(self._connect()) as db, db:
            row = db.execute(
                "SELECT * FROM desired_states WHERE state_sha256=?",
                (state_hash,),
            ).fetchone()
            if row is None:
                return None
            state = self._state_from_row(row)
        if state.state_sha256 != state_hash:  # defensive, _state_from_row binds it
            raise DesiredStateError("desired-state versiechecksum faalt")
        return state

    def current(self) -> dict[str, Any]:
        """Return privacy-minimal control metadata, never target coordinates."""

        with closing(self._connect()) as db, db:
            authority = self._load_authority(db)
            active = self._load_active(db)
            if active is None:
                result = {"method": METHOD, "status": "unconfigured"}
                if authority is not None:
                    result.update({
                        "producer_id": str(authority["producer_id"]),
                        "authority_epoch": int(authority["authority_epoch"]),
                    })
                return result
            state, row = active
            head = db.execute(
                "SELECT activated_at FROM active_desired_state WHERE singleton=1"
            ).fetchone()
        return {
            "method": METHOD,
            "status": "configured",
            "state_sha256": state.state_sha256,
            "producer_id": state.producer_id,
            "authority_epoch": state.authority_epoch,
            "generation": state.generation,
            "issued_at": state.issued_at,
            "first_received_at": float(row["first_received_at"]),
            "last_received_at": float(row["last_received_at"]),
            "receipt_count": int(row["receipt_count"]),
            "activated_at": float(head["activated_at"]),
            "target_count": len(state.targets),
        }


def desired_state_apply(job) -> dict[str, Any]:
    payload = job.payload
    if not isinstance(payload, dict) or set(payload) != {"desired_state"}:
        raise DesiredStateError("desired_state_apply vereist exact desired_state")
    return DesiredStateStore().apply(payload["desired_state"])


def desired_state_claim(job) -> dict[str, Any]:
    payload = job.payload
    if not isinstance(payload, dict) or set(payload) != {"producer_id", "claim_id"}:
        raise DesiredStateError(
            "desired_state_claim vereist exact producer_id en claim_id"
        )
    return DesiredStateStore().claim_producer(
        payload["producer_id"], payload["claim_id"]
    )


def desired_state_snapshot(job) -> dict[str, Any]:
    if job.payload != {}:
        raise DesiredStateError("desired_state_snapshot vereist exact een leeg object")
    return DesiredStateStore().current()


__all__ = [
    "CONTRACT",
    "DEFAULT_STORE_PATH",
    "DesiredState",
    "DesiredStateConflict",
    "DesiredStateError",
    "DesiredStateStore",
    "LEGACY_SCHEMA_VERSION",
    "MAX_AUTHORITY_CLAIMS",
    "MAX_CONTRACT_BYTES",
    "MAX_DATABASE_BYTES",
    "MAX_DYNAMIC_POSITION_AGE_SECONDS",
    "MAX_STATE_ROWS",
    "MAX_TARGETS",
    "STORE_IDENTITY_SCHEMA",
    "RuntimeSettings",
    "Target",
    "desired_state_apply",
    "desired_state_claim",
    "desired_state_snapshot",
    "parse_desired_state",
]
