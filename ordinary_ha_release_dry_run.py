"""Local proof for an ordinary-Home-Assistant integration-only release.

The release builder deliberately keeps producing and sealing the integration
and worker as one pair.  For integration 1.13.3 the worker side must be
reconstructed exclusively from the already sealed 1.16.2 baseline; the moving
all-source worker tree is not an admissible release input.  This module consumes
that unchanged pair-evidence contract, but its simulated installation copies
and extracts only the integration ZIP.

It has no live Home Assistant, Supervisor, network or SSH capability.  Every
target-side prerequisite therefore remains an explicit, false live gate in the
result.  The target must be reconfigured through the HA UI to exact ``legacy``
mode before local provider/OFE/StormEngine output is claimed.  In particular,
an old component ZIP is not presented as a functional rollback after that
explicit mode change.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import tempfile
from typing import Iterable, Mapping
import zipfile

from storm_tracker_worker.release_artifacts import (
    INTEGRATION_ROOT_NAME,
    ReleaseArtifactError,
    verify_release_candidate_evidence,
)


ORDINARY_HA_DRY_RUN_CONTRACT = (
    "stv3-ordinary-ha-integration-only-release-dry-run-v2"
)
ORDINARY_HA_DEPLOY_MODE = "ordinary_home_assistant_integration_only"
REQUIRED_CONFIG_ENTRY_VERSION = 3
REQUIRED_RUNTIME_MODE = "legacy"
REQUIRED_WORKER_ONLY_RUNTIME_MODE = "worker_only"
REQUIRED_LEGACY_REQUIREMENTS = (
    "h5py>=3.11,<4",
    "numpy>=2.0,<3",
    "pyproj>=3.6,<4",
)
SEALED_BASELINE_INTEGRATION_VERSION = "1.13.3"
SEALED_BASELINE_WORKER_VERSION = "1.16.2"
SEALED_BASELINE_WORKER_ARCHIVE_SHA256 = (
    "32a222a229a93ba2f61ef0c0d58caa32d532ea36ce2a1adcaff42165e2f0e275"
)
SEALED_BASELINE_WORKER_TREE_SHA256 = (
    "f3130552b899ae2ae3054b7365c09079ab2f944bc1937814a2fc9a3ab101b641"
)
MQTT_OFFLINE_WORKER_STATE = "offline"
MQTT_OFFLINE_WEATHER_STATE = "onvoldoende_data"
TRANSFER_DIRECTORY = "/homeassistant/storm_tracker_deploy"
CUSTOM_COMPONENTS_DIRECTORY = "/homeassistant/custom_components"
ACTIVE_COMPONENT_DIRECTORY = (
    f"{CUSTOM_COMPONENTS_DIRECTORY}/{INTEGRATION_ROOT_NAME}"
)
REFERENCE_DEPLOY_RUN_DIRECTORY = (
    f"{TRANSFER_DIRECTORY}/ordinary-ha-unique-run-id"
)
REPORT_FILENAME = "ordinary_ha_integration_only_dry_run_report.json"

_REQUIRED_NON_ACTIVE_COMPONENT_ROLES = frozenset({
    "predeploy_snapshot",
    "quarantine_failed_candidate",
    "staging_initial",
    "staging_same_version_recovery",
})
_REFERENCE_NON_ACTIVE_COMPONENT_DIRECTORIES = {
    "predeploy_snapshot": (
        f"{REFERENCE_DEPLOY_RUN_DIRECTORY}/predeploy-snapshot/"
        f"{INTEGRATION_ROOT_NAME}"
    ),
    "quarantine_failed_candidate": (
        f"{REFERENCE_DEPLOY_RUN_DIRECTORY}/quarantine/"
        f"{INTEGRATION_ROOT_NAME}.candidate_failed"
    ),
    "staging_initial": (
        f"{REFERENCE_DEPLOY_RUN_DIRECTORY}/staging-initial/"
        f"{INTEGRATION_ROOT_NAME}"
    ),
    "staging_same_version_recovery": (
        f"{REFERENCE_DEPLOY_RUN_DIRECTORY}/staging-same-version-recovery/"
        f"{INTEGRATION_ROOT_NAME}"
    ),
}

# These code versions predate, or do not safely reverse, the final schema-v3
# mode router.  In particular, 1.13.2 cannot load an entry after it has been
# explicitly reconfigured to ``legacy``.  They remain forensic inputs only.
NONFUNCTIONAL_CODE_ROLLBACK_VERSIONS = ("1.11.x", "1.13.1", "1.13.2")

LIVE_INSTALLATION_GATES = (
    "active_component_path_not_live_discovered",
    "active_component_snapshot_and_sha256_not_live_observed",
    "canonical_non_overlapping_component_layout_not_live_observed",
    "config_entry_disabled_not_live_observed",
    "config_entry_legacy_ui_reconfigure_not_live_observed",
    "exact_one_storm_tracker_manifest_below_custom_components_not_live_observed",
    "full_home_assistant_backup_not_live_observed",
    "home_assistant_restart_not_exercised",
    "legacy_local_provider_ofe_storm_nowcast_map_canary_not_live_observed",
    "ordinary_home_assistant_target_not_live_observed",
    "same_version_recovery_not_live_exercised",
    "uploaded_integration_sha256_not_live_observed",
    "warning_dispatch_default_off_and_opt_in_not_live_observed",
    "worker_absent_local_processing_not_live_observed",
)

_MAX_FILE_BYTES = 512 * 1024 * 1024
_MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
_REQUIRED_INTEGRATION_FILES = frozenset({
    "__init__.py",
    "config_flow.py",
    "const.py",
    "engine/region_manager.py",
    "manifest.json",
    "operational_snapshot_sensors.py",
    "operational_snapshot_views.py",
    "sensor.py",
    "warning_dispatcher.py",
    "worker_bridge.py",
    "worker_only_entry.py",
    "worker_only_runtime.py",
})


class OrdinaryHaDryRunError(RuntimeError):
    """The integration-only proof cannot continue without weakening a gate."""


@dataclass(frozen=True, slots=True)
class OrdinaryHaDryRunResult:
    report_path: Path
    report_file_sha256: str
    report: Mapping[str, object]


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise OrdinaryHaDryRunError("rapport is niet canoniek serialiseerbaar") from error


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _is_below(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return path != parent


def _canonical_absolute_posix_path(value: object, *, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value.startswith("/"):
        raise OrdinaryHaDryRunError(f"{label} moet een absoluut POSIX-pad zijn")
    if "\\" in value or "\x00" in value:
        raise OrdinaryHaDryRunError(f"{label} is geen canoniek POSIX-pad")
    parts = value.split("/")
    if (
        len(parts) < 2
        or parts[0] != ""
        or any(part in {"", ".", ".."} for part in parts[1:])
    ):
        raise OrdinaryHaDryRunError(f"{label} is geen canoniek POSIX-pad")
    path = PurePosixPath(value)
    if path.as_posix() != value:
        raise OrdinaryHaDryRunError(f"{label} is geen canoniek POSIX-pad")
    return path


def _is_posix_below(path: PurePosixPath, parent: PurePosixPath) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return path != parent


def _posix_paths_overlap(left: PurePosixPath, right: PurePosixPath) -> bool:
    return (
        left == right
        or _is_posix_below(left, right)
        or _is_posix_below(right, left)
    )


def verify_ordinary_ha_target_layout_contract(
    *,
    custom_components_directory: str,
    active_component_directory: str,
    deploy_run_directory: str,
    non_active_component_directories: Mapping[str, str],
    storm_tracker_manifest_paths_below_custom_components: Iterable[str],
) -> dict[str, object]:
    """Fail closed on unsafe ordinary-HA component placement.

    Inputs must already be resolved to their canonical target-side POSIX paths.
    The manifest iterable must be the exhaustive set whose parsed manifest
    domain is ``storm_tracker_v3`` below canonical ``custom_components``
    immediately before a Home Assistant restart. Other custom integrations are
    outside this domain-scoped inventory.
    """

    custom_components = _canonical_absolute_posix_path(
        custom_components_directory,
        label="custom_components-pad",
    )
    active_component = _canonical_absolute_posix_path(
        active_component_directory,
        label="actieve componentpad",
    )
    deploy_run = _canonical_absolute_posix_path(
        deploy_run_directory,
        label="deployrunpad",
    )
    transfer_directory = _canonical_absolute_posix_path(
        TRANSFER_DIRECTORY,
        label="vaste deploymap",
    )
    if (
        active_component.parent != custom_components
        or active_component.name != INTEGRATION_ROOT_NAME
    ):
        raise OrdinaryHaDryRunError(
            "actieve component moet exact de directe storm_tracker_v3-map "
            "onder custom_components zijn"
        )
    if not _is_posix_below(deploy_run, transfer_directory):
        raise OrdinaryHaDryRunError(
            "deployrun moet uniek en strikt onder de vaste deploymap liggen"
        )
    if _posix_paths_overlap(custom_components, deploy_run):
        raise OrdinaryHaDryRunError(
            "deployrun en custom_components mogen niet overlappen"
        )

    if not isinstance(non_active_component_directories, Mapping):
        raise OrdinaryHaDryRunError(
            "niet-actieve componentpaden zijn geen rollenmapping"
        )
    roles = set(non_active_component_directories)
    if roles != _REQUIRED_NON_ACTIVE_COMPONENT_ROLES:
        raise OrdinaryHaDryRunError(
            "niet-actieve componentrollen zijn onvolledig of onverwacht"
        )
    non_active: dict[str, PurePosixPath] = {}
    for role in sorted(_REQUIRED_NON_ACTIVE_COMPONENT_ROLES):
        path = _canonical_absolute_posix_path(
            non_active_component_directories[role],
            label=f"{role}-componentpad",
        )
        if _posix_paths_overlap(path, custom_components):
            raise OrdinaryHaDryRunError(
                f"{role}-componentpad mag niet onder custom_components liggen"
            )
        if not _is_posix_below(path, deploy_run):
            raise OrdinaryHaDryRunError(
                f"{role}-componentpad moet strikt onder de deployrun liggen"
            )
        non_active[role] = path

    component_directories = {
        "active": active_component,
        **non_active,
    }
    ordered_roles = sorted(component_directories)
    for index, left_role in enumerate(ordered_roles):
        for right_role in ordered_roles[index + 1:]:
            if _posix_paths_overlap(
                component_directories[left_role],
                component_directories[right_role],
            ):
                raise OrdinaryHaDryRunError(
                    "componentpaden overlappen: "
                    f"{left_role} en {right_role}"
                )

    if isinstance(
        storm_tracker_manifest_paths_below_custom_components,
        (str, bytes),
    ):
        raise OrdinaryHaDryRunError(
            "manifestinventaris onder custom_components is ongeldig"
        )
    manifests = tuple(
        _canonical_absolute_posix_path(
            path,
            label="manifestpad onder custom_components",
        )
        for path in storm_tracker_manifest_paths_below_custom_components
    )
    expected_manifest = active_component / "manifest.json"
    if len(manifests) != 1 or manifests[0] != expected_manifest:
        raise OrdinaryHaDryRunError(
            "exact één Storm Tracker-manifest onder custom_components is vereist: "
            "storm_tracker_v3/manifest.json"
        )

    return {
        "active_component_directory": active_component.as_posix(),
        "active_manifest_path": expected_manifest.as_posix(),
        "canonical_paths_verified": True,
        "custom_components_directory": custom_components.as_posix(),
        "deploy_run_directory": deploy_run.as_posix(),
        "exact_one_storm_tracker_manifest_below_custom_components_verified": True,
        "storm_tracker_manifest_paths_below_custom_components": [
            expected_manifest.as_posix()
        ],
        "non_active_component_directories": {
            role: non_active[role].as_posix() for role in sorted(non_active)
        },
        "non_active_components_outside_custom_components_verified": True,
        "pairwise_non_overlapping_component_directories_verified": True,
    }


def _create_run_root(argument: Path) -> Path:
    raw = Path(argument)
    if raw.exists() or raw.is_symlink():
        raise OrdinaryHaDryRunError("run-root moet nieuw en afwezig zijn")
    parent_argument = raw.parent
    if parent_argument.is_symlink():
        raise OrdinaryHaDryRunError("run-rootparent mag geen symlink zijn")
    try:
        parent = parent_argument.resolve(strict=True)
    except OSError as error:
        raise OrdinaryHaDryRunError("run-rootparent bestaat niet") from error
    if not parent.is_dir():
        raise OrdinaryHaDryRunError("run-rootparent is geen map")
    destination = parent / raw.name
    project_analysis = (Path(__file__).resolve().parent / ".analysis").resolve()
    temporary = Path(tempfile.gettempdir()).resolve()
    if not (
        _is_below(destination, project_analysis)
        or _is_below(destination, temporary)
    ):
        raise OrdinaryHaDryRunError(
            "run-root moet onder project-.analysis of de OS-tempmap liggen"
        )
    try:
        destination.mkdir(mode=0o700, exist_ok=False)
    except OSError as error:
        raise OrdinaryHaDryRunError("run-root kon niet exclusief worden gemaakt") from error
    return destination


def _tree_files(evidence: Mapping[str, object]) -> tuple[dict[str, object], ...]:
    try:
        tree = evidence["source_trees"]["integration"]
        raw_files = tree["files"]
        declared_count = tree["file_count"]
        declared_total = tree["total_bytes"]
    except (KeyError, TypeError) as error:
        raise OrdinaryHaDryRunError("release-evidence mist integration-tree") from error
    if not isinstance(raw_files, list) or len(raw_files) != declared_count:
        raise OrdinaryHaDryRunError("integration-tree file_count klopt niet")
    files: list[dict[str, object]] = []
    total = 0
    paths: list[str] = []
    for item in raw_files:
        if not isinstance(item, dict) or set(item) != {
            "mode", "path", "sha256", "size"
        }:
            raise OrdinaryHaDryRunError("integration-treebestand is ongeldig")
        path = item["path"]
        size = item["size"]
        sha256 = item["sha256"]
        mode = item["mode"]
        if (
            not isinstance(path, str)
            or not path
            or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or "\\" in path
            or type(size) is not int
            or size < 0
            or size > _MAX_FILE_BYTES
            or not isinstance(sha256, str)
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
            or mode not in (0o644, 0o755)
        ):
            raise OrdinaryHaDryRunError("integration-treebestand is ongeldig")
        paths.append(path)
        total += size
        files.append(dict(item))
    if (
        paths != sorted(paths)
        or len(set(paths)) != len(paths)
        or total != declared_total
        or total > _MAX_TOTAL_BYTES
    ):
        raise OrdinaryHaDryRunError("integration-tree is niet canoniek")
    return tuple(files)


def _copy_integration_archive(
    source: Path,
    destination: Path,
    *,
    expected_size: int,
    expected_sha256: str,
) -> None:
    if destination.exists() or destination.is_symlink():
        raise OrdinaryHaDryRunError("lokale integrationspool bestaat al")
    digest = hashlib.sha256()
    total = 0
    try:
        with source.open("rb") as reader, destination.open("xb") as writer:
            while chunk := reader.read(1024 * 1024):
                total += len(chunk)
                if total > expected_size:
                    raise OrdinaryHaDryRunError(
                        "integratiearchief groeide tijdens lokale binding"
                    )
                digest.update(chunk)
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
    except OrdinaryHaDryRunError:
        raise
    except OSError as error:
        raise OrdinaryHaDryRunError(
            "integratiearchief kon niet lokaal worden gebonden"
        ) from error
    if total != expected_size or digest.hexdigest() != expected_sha256:
        raise OrdinaryHaDryRunError(
            "lokaal gebonden integratiearchief verschilt van pair-evidence"
        )


def _extract_exact_integration(
    archive: Path,
    destination_parent: Path,
    files: Iterable[Mapping[str, object]],
) -> Path:
    destination_parent.mkdir(mode=0o700, exist_ok=False)
    package_root = destination_parent / INTEGRATION_ROOT_NAME
    package_root.mkdir(mode=0o700, exist_ok=False)
    materialized = tuple(files)
    expected_members = {
        f"{INTEGRATION_ROOT_NAME}/{item['path']}" for item in materialized
    }
    try:
        with zipfile.ZipFile(archive, "r") as bundle:
            actual_files = {
                item.filename for item in bundle.infolist() if not item.is_dir()
            }
            if actual_files != expected_members:
                raise OrdinaryHaDryRunError(
                    "integratiearchief heeft na binding een andere memberset"
                )
            for record in materialized:
                relative = str(record["path"])
                member_name = f"{INTEGRATION_ROOT_NAME}/{relative}"
                target = package_root.joinpath(*PurePosixPath(relative).parts)
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                if target.exists() or target.is_symlink():
                    raise OrdinaryHaDryRunError(
                        "integratie-extractiedoel bestaat al"
                    )
                digest = hashlib.sha256()
                total = 0
                with bundle.open(member_name, "r") as source, target.open("xb") as sink:
                    while chunk := source.read(1024 * 1024):
                        total += len(chunk)
                        if total > record["size"]:
                            raise OrdinaryHaDryRunError(
                                "integratiemember overschrijdt evidencegrootte"
                            )
                        digest.update(chunk)
                        sink.write(chunk)
                    sink.flush()
                    os.fsync(sink.fileno())
                if total != record["size"] or digest.hexdigest() != record["sha256"]:
                    raise OrdinaryHaDryRunError(
                        "integratiemember verschilt van pair-evidence"
                    )
                if os.name != "nt":
                    os.chmod(target, int(record["mode"]))
    except OrdinaryHaDryRunError:
        raise
    except (KeyError, OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise OrdinaryHaDryRunError(
            "integratiearchief kon niet exact worden gematerialiseerd"
        ) from error
    return package_root


def _verify_materialized_tree(
    package_root: Path,
    files: Iterable[Mapping[str, object]],
) -> None:
    expected = {str(item["path"]): item for item in files}
    actual: dict[str, Path] = {}
    for root, directories, filenames in os.walk(package_root, followlinks=False):
        root_path = Path(root)
        for name in directories:
            directory = root_path / name
            if directory.is_symlink():
                raise OrdinaryHaDryRunError("gematerialiseerde tree bevat symlink")
        for name in filenames:
            candidate = root_path / name
            if candidate.is_symlink() or not candidate.is_file():
                raise OrdinaryHaDryRunError(
                    "gematerialiseerde tree bevat geen regulier bestand"
                )
            actual[candidate.relative_to(package_root).as_posix()] = candidate
    if set(actual) != set(expected):
        raise OrdinaryHaDryRunError("gematerialiseerde integration-tree wijkt af")
    for relative, record in expected.items():
        path = actual[relative]
        details = path.stat()
        if details.st_size != record["size"] or _sha256_file(path) != record["sha256"]:
            raise OrdinaryHaDryRunError("gematerialiseerd bestand wijkt af")
        if os.name != "nt" and stat.S_IMODE(details.st_mode) != record["mode"]:
            raise OrdinaryHaDryRunError("gematerialiseerde bestandsmodus wijkt af")


def _read_source(component: Path, relative: str) -> tuple[str, ast.Module]:
    path = component / relative
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        return text, ast.parse(text, filename=relative)
    except (OSError, UnicodeError, SyntaxError) as error:
        raise OrdinaryHaDryRunError(
            f"runtimecontractbestand {relative} is ongeldig"
        ) from error


def _top_level_literal(module: ast.Module, name: str) -> object:
    matches: list[ast.expr] = []
    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            matches.append(node.value)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
            and node.value is not None
        ):
            matches.append(node.value)
    if len(matches) != 1:
        raise OrdinaryHaDryRunError(f"runtimeconstante {name} is niet uniek")
    try:
        return ast.literal_eval(matches[0])
    except (ValueError, TypeError) as error:
        raise OrdinaryHaDryRunError(
            f"runtimeconstante {name} is niet letterlijk"
        ) from error


def _function(module: ast.Module, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    matches = [
        node for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]
    if len(matches) != 1:
        raise OrdinaryHaDryRunError(f"runtimefunctie {name} is niet uniek")
    return matches[0]


def _called_names(node: ast.AST) -> frozenset[str]:
    names: set[str] = set()
    for nested in ast.walk(node):
        if not isinstance(nested, ast.Call):
            continue
        if isinstance(nested.func, ast.Name):
            names.add(nested.func.id)
        elif isinstance(nested.func, ast.Attribute):
            names.add(nested.func.attr)
    return frozenset(names)


def _referenced_names(node: ast.AST) -> frozenset[str]:
    return frozenset(
        nested.id for nested in ast.walk(node) if isinstance(nested, ast.Name)
    )


def _string_constants(node: ast.AST) -> frozenset[str]:
    return frozenset(
        nested.value
        for nested in ast.walk(node)
        if isinstance(nested, ast.Constant) and isinstance(nested.value, str)
    )


def _class_literal(module: ast.Module, class_name: str, name: str) -> object:
    classes = [
        node for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    ]
    if len(classes) != 1:
        raise OrdinaryHaDryRunError(f"runtimeclass {class_name} is niet uniek")
    scoped = ast.Module(body=list(classes[0].body), type_ignores=[])
    return _top_level_literal(scoped, name)


def _assignment_referenced_names(
    module: ast.Module,
    name: str,
) -> frozenset[str]:
    matches: list[ast.AST] = []
    for statement in module.body:
        if isinstance(statement, ast.Assign):
            if any(
                isinstance(target, ast.Name) and target.id == name
                for target in statement.targets
            ):
                matches.append(statement.value)
        elif (
            isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and statement.target.id == name
        ):
            matches.append(statement.value)
    if len(matches) != 1 or matches[0] is None:
        raise OrdinaryHaDryRunError(f"runtimeconstante {name} is niet uniek")
    return _referenced_names(matches[0])


def _class_method(
    module: ast.Module,
    class_name: str,
    method_name: str,
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    classes = [
        node for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    ]
    if len(classes) != 1:
        raise OrdinaryHaDryRunError(f"runtimeclass {class_name} is niet uniek")
    methods = [
        node for node in classes[0].body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
    ]
    if len(methods) != 1:
        raise OrdinaryHaDryRunError(
            f"runtimemethode {class_name}.{method_name} is niet uniek"
        )
    return methods[0]


def _nested_function(
    module: ast.Module,
    class_name: str,
    method_name: str,
    function_name: str,
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    method = _class_method(module, class_name, method_name)
    matches = [
        node for node in method.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function_name
    ]
    if len(matches) != 1:
        raise OrdinaryHaDryRunError(
            "runtimefunctie "
            f"{class_name}.{method_name}.{function_name} is niet uniek"
        )
    return matches[0]


def _call_keyword_literals(
    node: ast.AST,
    *,
    call_name: str,
    keyword_name: str,
) -> tuple[object, ...]:
    values: list[object] = []
    for nested in ast.walk(node):
        if not isinstance(nested, ast.Call):
            continue
        called = None
        if isinstance(nested.func, ast.Name):
            called = nested.func.id
        elif isinstance(nested.func, ast.Attribute):
            called = nested.func.attr
        if called != call_name:
            continue
        for keyword in nested.keywords:
            if keyword.arg != keyword_name:
                continue
            try:
                values.append(ast.literal_eval(keyword.value))
            except (ValueError, TypeError) as error:
                raise OrdinaryHaDryRunError(
                    f"{call_name}.{keyword_name} is geen vaste literal"
                ) from error
    return tuple(values)


def _inspect_static_ordinary_ha_contract(
    component: Path,
    *,
    integration_version: str,
) -> dict[str, object]:
    missing = sorted(
        relative for relative in _REQUIRED_INTEGRATION_FILES
        if not (component / relative).is_file()
    )
    if missing:
        raise OrdinaryHaDryRunError(
            "integration mist ordinary-HA contractbestanden: " + ", ".join(missing)
        )
    try:
        manifest = json.loads((component / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise OrdinaryHaDryRunError("integrationmanifest is ongeldig") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("version") != integration_version
        or manifest.get("config_flow") is not True
        or manifest.get("requirements") != list(REQUIRED_LEGACY_REQUIREMENTS)
        or manifest.get("dependencies") != ["mqtt"]
        or manifest.get("iot_class") != "local_push"
    ):
        raise OrdinaryHaDryRunError(
            "manifest bewijst niet de exacte normale-HA legacy requirements"
        )

    _init_text, init_module = _read_source(component, "__init__.py")
    _flow_text, flow_module = _read_source(component, "config_flow.py")
    _const_text, const_module = _read_source(component, "const.py")
    _bridge_text, bridge_module = _read_source(component, "worker_bridge.py")
    _region_text, region_module = _read_source(
        component, "engine/region_manager.py"
    )
    _warning_text, warning_module = _read_source(
        component, "warning_dispatcher.py"
    )
    _views_text, views_module = _read_source(
        component, "operational_snapshot_views.py"
    )
    _sensors_text, sensors_module = _read_source(
        component, "operational_snapshot_sensors.py"
    )

    if _top_level_literal(init_module, "CONFIG_ENTRY_VERSION") != 3:
        raise OrdinaryHaDryRunError("integration is niet config-entry schema-v3")
    if (
        _class_literal(flow_module, "StormTrackerConfigFlow", "VERSION")
        != REQUIRED_CONFIG_ENTRY_VERSION
    ):
        raise OrdinaryHaDryRunError("ConfigFlow is niet schema-v3")
    if (
        _top_level_literal(const_module, "RUNTIME_MODE_WORKER_ONLY")
        != REQUIRED_WORKER_ONLY_RUNTIME_MODE
    ):
        raise OrdinaryHaDryRunError("worker_only runtimeconstante ontbreekt")
    if (
        _top_level_literal(const_module, "RUNTIME_MODE_LEGACY")
        != REQUIRED_RUNTIME_MODE
    ):
        raise OrdinaryHaDryRunError("legacy runtimeconstante ontbreekt")
    if (
        _top_level_literal(
            const_module, "CONF_WARNING_NOTIFICATIONS_ENABLED"
        ) != "warning_notifications_enabled"
        or _top_level_literal(const_module, "CONF_NOTIFY_SERVICE")
        != "notify_service"
    ):
        raise OrdinaryHaDryRunError(
            "expliciete warning-opt-inconfiguratie ontbreekt"
        )

    setup = _function(init_module, "async_setup_entry")
    setup_calls = _called_names(setup)
    setup_names = _referenced_names(setup)
    if (
        "async_setup_worker_only_entry" not in setup_calls
        or "_async_setup_legacy_entry" not in setup_calls
        or "_async_setup_runtime" in setup_calls
        or not {
            "CONFIG_ENTRY_VERSION",
            "RUNTIME_MODE_LEGACY",
            "RUNTIME_MODE_WORKER_ONLY",
            "_explicit_entry_mode",
        } <= setup_names
    ):
        raise OrdinaryHaDryRunError(
            "schema-v3 setup routeert niet exact expliciet legacy/"
            "exclusief worker_only"
        )
    migration = _function(init_module, "async_migrate_entry")
    if (
        "async_update_entry" not in _called_names(migration)
        or not {
            "CONFIG_ENTRY_VERSION",
            "RUNTIME_MODE_LEGACY",
            "RUNTIME_MODE_WORKER_ONLY",
        }
        <= _referenced_names(migration)
    ):
        raise OrdinaryHaDryRunError(
            "schema-v3 migratie bewaart geen expliciete dual-modekeuze"
        )

    runtime = _function(init_module, "_async_setup_runtime")
    runtime_names = _referenced_names(runtime)
    if (
        not {"StormManager", "WarningDispatcher", "async_initialize"}
        <= _called_names(runtime)
        or not {
            "CONF_NOTIFY_SERVICE",
            "CONF_WARNING_NOTIFICATIONS_ENABLED",
        } <= runtime_names
        or _call_keyword_literals(
            runtime,
            call_name="StormManager",
            keyword_name="allow_local_processing",
        ) != (True,)
        or (
            "conf.get(CONF_WARNING_NOTIFICATIONS_ENABLED) is True"
            not in ast.unparse(runtime)
        )
    ):
        raise OrdinaryHaDryRunError(
            "legacy runtime bewijst lokale verwerking en warning opt-in niet"
        )

    manager_init = _class_method(region_module, "StormManager", "__init__")
    keyword_defaults = dict(zip(
        (item.arg for item in manager_init.args.kwonlyargs),
        manager_init.args.kw_defaults,
    ))
    local_default = keyword_defaults.get("allow_local_processing")
    if not (
        isinstance(local_default, ast.Constant)
        and local_default.value is False
    ):
        raise OrdinaryHaDryRunError(
            "lokale StormManager-verwerking is niet exact opt-in default false"
        )
    local_batch = _nested_function(
        region_module,
        "StormManager",
        "_create_engine",
        "on_batch",
    )
    if (
        "process_batch" not in _called_names(local_batch)
        or "_allow_local_processing" not in {
            nested.attr for nested in ast.walk(local_batch)
            if isinstance(nested, ast.Attribute)
        }
    ):
        raise OrdinaryHaDryRunError(
            "StormManager heeft geen expliciete lokale batchroute"
        )

    worker_fields = _assignment_referenced_names(
        flow_module, "WORKER_ONLY_SCHEMA_FIELDS"
    )
    legacy_fields = _assignment_referenced_names(
        flow_module, "LEGACY_SCHEMA_FIELDS"
    )
    warning_fields = {
        "CONF_NOTIFY_SERVICE",
        "CONF_WARNING_NOTIFICATIONS_ENABLED",
    }
    legacy_schema = _function(flow_module, "_schema")
    if (
        warning_fields & worker_fields
        or not warning_fields <= legacy_fields
        or not warning_fields <= _referenced_names(legacy_schema)
        or (
            "defaults.get(CONF_WARNING_NOTIFICATIONS_ENABLED, False)"
            not in ast.unparse(legacy_schema)
        )
    ):
        raise OrdinaryHaDryRunError(
            "warningconfig is niet legacy-only en standaard uitgeschakeld"
        )

    warning_dispatch = _class_method(
        warning_module, "WarningDispatcher", "_async_dispatch"
    )
    warning_payload = _class_method(
        warning_module, "WarningDispatcher", "_notification_payload"
    )
    warning_process = _class_method(
        warning_module, "WarningDispatcher", "async_process"
    )
    if (
        not {
            "async_call",
            "async_fire",
            "_async_save",
            "_notification_payload",
        }
        <= _called_names(warning_dispatch)
        or "WARNING_SENT_EVENT" not in _referenced_names(warning_dispatch)
        or "official_warning" not in _string_constants(warning_payload)
        or "_async_dispatch" not in _called_names(warning_process)
    ):
        raise OrdinaryHaDryRunError(
            "warningdispatcher bewijst service, event en persistence niet"
        )

    bridge_setup = _function(bridge_module, "async_setup_worker_bridge")
    if (
        not {"async_wait_for_mqtt_client", "_mark_operational_snapshot_worker_offline"}
        <= _called_names(bridge_setup)
        or MQTT_OFFLINE_WORKER_STATE not in _string_constants(bridge_setup)
    ):
        raise OrdinaryHaDryRunError(
            "MQTT-afwezigheid wordt niet aantoonbaar offline geprojecteerd"
        )
    current_states = _top_level_literal(views_module, "_CURRENT_STATE")
    if (
        not isinstance(current_states, dict)
        or current_states.get("unknown") != MQTT_OFFLINE_WEATHER_STATE
    ):
        raise OrdinaryHaDryRunError(
            "onbekend weer wordt niet als onvoldoende_data gepresenteerd"
        )
    fail_closed_methods = [
        node for node in ast.walk(sensors_module)
        if isinstance(node, ast.FunctionDef) and node.name == "_fail_closed"
    ]
    if (
        len(fail_closed_methods) != 1
        or MQTT_OFFLINE_WEATHER_STATE not in _string_constants(fail_closed_methods[0])
    ):
        raise OrdinaryHaDryRunError(
            "snapshotsensor heeft geen fail-closed onvoldoende_data-projectie"
        )
    return {
        "config_entry_schema_version": REQUIRED_CONFIG_ENTRY_VERSION,
        "legacy_local_processing": True,
        "manifest_dependencies": ["mqtt"],
        "manifest_requirements": list(REQUIRED_LEGACY_REQUIREMENTS),
        "mqtt_absent_weather_state": MQTT_OFFLINE_WEATHER_STATE,
        "mqtt_absent_worker_state": MQTT_OFFLINE_WORKER_STATE,
        "runtime_mode": REQUIRED_RUNTIME_MODE,
        "supported_runtime_modes": [
            REQUIRED_RUNTIME_MODE,
            REQUIRED_WORKER_ONLY_RUNTIME_MODE,
        ],
        "static_contract_verified": True,
        "warning_dispatch": "explicit_opt_in_default_off",
        "worker_archive_installed": False,
        "worker_only_default_retained": True,
    }


def _move_new_directory(source: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise OrdinaryHaDryRunError("dry-run generatiebestemming bestaat al")
    try:
        source.rename(destination)
    except OSError as error:
        raise OrdinaryHaDryRunError(
            "dry-run generatie kon niet worden geactiveerd"
        ) from error


def _storm_tracker_manifests_below(directory: Path) -> tuple[Path, ...]:
    manifests: list[Path] = []
    for root, directories, filenames in os.walk(directory, followlinks=False):
        root_path = Path(root)
        for name in directories:
            if (root_path / name).is_symlink():
                raise OrdinaryHaDryRunError(
                    "gesimuleerde custom_components bevat een symlinkmap"
                )
        for name in filenames:
            path = root_path / name
            if path.is_symlink() or not path.is_file():
                raise OrdinaryHaDryRunError(
                    "gesimuleerde custom_components bevat geen regulier bestand"
                )
            if name == "manifest.json":
                try:
                    manifest = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as error:
                    raise OrdinaryHaDryRunError(
                        "gesimuleerde custom integration heeft een ongeldig manifest"
                    ) from error
                if (
                    isinstance(manifest, dict)
                    and manifest.get("domain") == INTEGRATION_ROOT_NAME
                ):
                    manifests.append(path.resolve(strict=True))
    return tuple(sorted(manifests, key=lambda path: path.as_posix()))


def _verify_simulated_target_layout(
    *,
    custom_components: Path,
    active_component: Path,
    deploy_run: Path,
    retained_non_active_components: Iterable[Path],
) -> None:
    try:
        canonical_custom_components = custom_components.resolve(strict=True)
        canonical_active = active_component.resolve(strict=True)
        canonical_deploy_run = deploy_run.resolve(strict=True)
        canonical_non_active = tuple(
            path.resolve(strict=True) for path in retained_non_active_components
        )
    except OSError as error:
        raise OrdinaryHaDryRunError(
            "gesimuleerde doelpaden konden niet canoniek worden bepaald"
        ) from error
    if (
        canonical_active.parent != canonical_custom_components
        or canonical_active.name != INTEGRATION_ROOT_NAME
    ):
        raise OrdinaryHaDryRunError(
            "gesimuleerde actieve component staat niet exact onder custom_components"
        )
    for path in canonical_non_active:
        if not _is_below(path, canonical_deploy_run):
            raise OrdinaryHaDryRunError(
                "gesimuleerde niet-actieve component staat buiten de deployrun"
            )
        if (
            _is_below(path, canonical_custom_components)
            or path == canonical_custom_components
        ):
            raise OrdinaryHaDryRunError(
                "gesimuleerde niet-actieve component staat onder custom_components"
            )
    all_components = (canonical_active, *canonical_non_active)
    for index, left in enumerate(all_components):
        for right in all_components[index + 1:]:
            if left == right or _is_below(left, right) or _is_below(right, left):
                raise OrdinaryHaDryRunError(
                    "gesimuleerde componentpaden overlappen"
                )
    manifests = _storm_tracker_manifests_below(canonical_custom_components)
    expected_manifest = (canonical_active / "manifest.json").resolve(strict=True)
    if manifests != (expected_manifest,):
        raise OrdinaryHaDryRunError(
            "gesimuleerde custom_components bevat niet exact het actieve manifest"
        )


def _sealed_worker_baseline_contract(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    """Bind release 1.13.3 to the unchanged, already sealed worker baseline."""

    required = evidence.get("integration_version") == (
        SEALED_BASELINE_INTEGRATION_VERSION
    )
    if not required:
        return {
            "applies_to_integration_version": (
                SEALED_BASELINE_INTEGRATION_VERSION
            ),
            "required_for_candidate": False,
            "verification_state": "not_applicable",
            "worker_archive_sha256": (
                SEALED_BASELINE_WORKER_ARCHIVE_SHA256
            ),
            "worker_source_tree_sha256": (
                SEALED_BASELINE_WORKER_TREE_SHA256
            ),
            "worker_version": SEALED_BASELINE_WORKER_VERSION,
        }
    try:
        worker_metadata = evidence["archives"]["worker"]
        worker_tree = evidence["source_trees"]["worker"]
        worker_version = evidence["worker_version"]
    except (KeyError, TypeError) as error:
        raise OrdinaryHaDryRunError(
            "sealed worker-baselinebewijs is onvolledig"
        ) from error
    if (
        worker_version != SEALED_BASELINE_WORKER_VERSION
        or worker_metadata.get("sha256")
        != SEALED_BASELINE_WORKER_ARCHIVE_SHA256
        or worker_tree.get("content_sha256")
        != SEALED_BASELINE_WORKER_TREE_SHA256
    ):
        raise OrdinaryHaDryRunError(
            "1.13.3 is niet gebouwd uit de exact sealed worker-1.16.2-baseline"
        )
    return {
        "applies_to_integration_version": SEALED_BASELINE_INTEGRATION_VERSION,
        "required_for_candidate": True,
        "verification_state": "verified",
        "worker_archive_sha256": SEALED_BASELINE_WORKER_ARCHIVE_SHA256,
        "worker_source_tree_sha256": SEALED_BASELINE_WORKER_TREE_SHA256,
        "worker_version": SEALED_BASELINE_WORKER_VERSION,
    }


def execute_ordinary_ha_integration_only_dry_run(
    *,
    candidate_evidence: Path,
    run_root: Path,
) -> OrdinaryHaDryRunResult:
    """Verify a pair release and simulate integration-only same-version recovery."""

    evidence_argument = Path(candidate_evidence)
    try:
        evidence_path = evidence_argument.resolve(strict=True)
        evidence = verify_release_candidate_evidence(evidence_path)
    except (OSError, ReleaseArtifactError) as error:
        raise OrdinaryHaDryRunError(
            "pair/RAW-compatibele release-evidence werd geweigerd"
        ) from error
    archives = evidence["archives"]
    integration_metadata = archives["integration"]
    worker_metadata = archives["worker"]
    sealed_worker_baseline = _sealed_worker_baseline_contract(evidence)
    integration_archive = evidence_path.parent / integration_metadata["filename"]
    files = _tree_files(evidence)

    target_layout_contract = verify_ordinary_ha_target_layout_contract(
        custom_components_directory=CUSTOM_COMPONENTS_DIRECTORY,
        active_component_directory=ACTIVE_COMPONENT_DIRECTORY,
        deploy_run_directory=REFERENCE_DEPLOY_RUN_DIRECTORY,
        non_active_component_directories=(
            _REFERENCE_NON_ACTIVE_COMPONENT_DIRECTORIES
        ),
        storm_tracker_manifest_paths_below_custom_components=(
            f"{ACTIVE_COMPONENT_DIRECTORY}/manifest.json",
        ),
    )

    root = _create_run_root(Path(run_root))
    simulated_home_assistant = root / "simulated-target" / "homeassistant"
    custom_components = simulated_home_assistant / "custom_components"
    deploy_run = (
        simulated_home_assistant
        / "storm_tracker_deploy"
        / "ordinary-ha-unique-run-id"
    )
    custom_components.mkdir(mode=0o700, parents=True, exist_ok=False)
    deploy_run.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name in ("quarantine", "spool"):
        (deploy_run / name).mkdir(mode=0o700, exist_ok=False)
    bound_archive = deploy_run / "spool" / integration_metadata["filename"]
    _copy_integration_archive(
        integration_archive,
        bound_archive,
        expected_size=int(integration_metadata["bytes"]),
        expected_sha256=str(integration_metadata["sha256"]),
    )

    initial_stage = _extract_exact_integration(
        bound_archive,
        deploy_run / "staging-initial",
        files,
    )
    _verify_materialized_tree(initial_stage, files)
    static_contract = _inspect_static_ordinary_ha_contract(
        initial_stage,
        integration_version=str(evidence["integration_version"]),
    )
    predeploy_snapshot = _extract_exact_integration(
        bound_archive,
        deploy_run / "predeploy-snapshot",
        files,
    )
    _verify_materialized_tree(predeploy_snapshot, files)
    active = custom_components / INTEGRATION_ROOT_NAME
    _move_new_directory(initial_stage, active)
    _verify_materialized_tree(active, files)

    # A failed live start must first quarantine the code and keep the config
    # entry disabled.  The only code-only recovery modelled here restores the
    # exact same candidate archive; it never substitutes an old schema build.
    quarantined = deploy_run / "quarantine" / (
        f"{INTEGRATION_ROOT_NAME}.candidate_failed"
    )
    _move_new_directory(active, quarantined)
    recovered_stage = _extract_exact_integration(
        bound_archive,
        deploy_run / "staging-same-version-recovery",
        files,
    )
    _verify_materialized_tree(recovered_stage, files)
    _move_new_directory(recovered_stage, active)
    _verify_materialized_tree(active, files)
    if _sha256_file(bound_archive) != integration_metadata["sha256"]:
        raise OrdinaryHaDryRunError(
            "same-version recoveryarchief veranderde tijdens simulatie"
        )
    _verify_simulated_target_layout(
        custom_components=custom_components,
        active_component=active,
        deploy_run=deploy_run,
        retained_non_active_components=(predeploy_snapshot, quarantined),
    )

    unsigned_report: dict[str, object] = {
        "blocker_namespaces": {
            "local": [],
            "live": list(LIVE_INSTALLATION_GATES),
        },
        "contract": ORDINARY_HA_DRY_RUN_CONTRACT,
        "deployment_mode": ORDINARY_HA_DEPLOY_MODE,
        "deployment_performed": False,
        "integration_installation_only": True,
        "live_installation_ready": False,
        "live_paths_touched": False,
        "local_simulation": {
            "canonical_non_overlapping_layout_verified": True,
            "candidate_installed": True,
            "candidate_quarantined": True,
            "config_entry_state_during_install": "disabled",
            "config_entry_state_during_recovery": "disabled",
            "config_entry_live_reconfigure_simulated": False,
            "exact_one_storm_tracker_manifest_below_custom_components_verified": True,
            "integration_tree_sha256": evidence["source_trees"]["integration"][
                "content_sha256"
            ],
            "local_simulation_passed": True,
            "same_version_recovered": True,
            "support_component_directories_outside_custom_components": True,
            "worker_archive_copied": False,
            "worker_archive_extracted": False,
            "worker_archive_installed": False,
        },
        "network_performed": False,
        "pair_release_evidence": {
            "contract": evidence["contract"],
            "evidence_sha256": evidence["evidence_sha256"],
            "integration": {
                "bytes": integration_metadata["bytes"],
                "filename": integration_metadata["filename"],
                "sha256": integration_metadata["sha256"],
                "version": evidence["integration_version"],
            },
            "pair_fully_verified": True,
            "sealed_worker_baseline": sealed_worker_baseline,
            "worker": {
                "bytes": worker_metadata["bytes"],
                "filename": worker_metadata["filename"],
                "installed": False,
                "read_only_pair_verification_only": True,
                "sha256": worker_metadata["sha256"],
                "version": evidence["worker_version"],
            },
        },
        "predeploy_requirements": {
            "active_component_path_live_discovery_required": True,
            "active_component_snapshot_is_functional_rollback": False,
            "active_component_snapshot_required": True,
            "active_component_snapshot_sha256_required": True,
            "canonical_target_paths_required": True,
            "config_entry_disabled_before_component_replacement_required": True,
            "config_entry_legacy_ui_reconfigure_required": True,
            "deploy_run_must_not_overlap_custom_components": True,
            "exact_one_active_storm_tracker_manifest_before_every_restart_required": True,
            "full_home_assistant_backup_required": True,
            "live_requirements_observed": False,
            "non_active_component_directories_under_deploy_run_required": True,
            "predeploy_staging_quarantine_under_custom_components_forbidden": True,
            "required_runtime_mode": REQUIRED_RUNTIME_MODE,
            "transfer_directory": TRANSFER_DIRECTORY,
            "uploaded_integration_sha256_match_required": True,
            "warning_notifications_default_off_required": True,
            "worker_installation_forbidden_for_this_route": True,
        },
        "target_filesystem_contract": {
            **target_layout_contract,
            "live_observed": False,
            "required_before_every_home_assistant_restart": True,
            "reference_layout_only": True,
        },
        "recovery": {
            "active_component_snapshot_is_forensic_only": True,
            "failure_action": (
                "keep_config_entry_disabled_and_quarantine_failed_component"
            ),
            "functional_old_code_rollback_claimed": False,
            "nonfunctional_code_rollback_versions": list(
                NONFUNCTIONAL_CODE_ROLLBACK_VERSIONS
            ),
            "pre_schema_v3_recovery_requires_full_ha_backup_restore": True,
            "same_version_recovery": {
                "archive_filename": integration_metadata["filename"],
                "archive_sha256": integration_metadata["sha256"],
                "config_entry_must_remain_disabled": True,
                "integration_version": evidence["integration_version"],
                "live_exercised": False,
                "local_simulated": True,
                "procedure_name": "exact_same_version_integration_recovery",
            },
        },
        "static_runtime_contract": static_contract,
    }
    report_sha256 = _sha256_bytes(_canonical(unsigned_report))
    report = {**unsigned_report, "report_sha256": report_sha256}
    report_path = root / REPORT_FILENAME
    report_bytes = _canonical(report) + b"\n"
    try:
        with report_path.open("xb") as target:
            target.write(report_bytes)
            target.flush()
            os.fsync(target.fileno())
    except OSError as error:
        raise OrdinaryHaDryRunError("dry-runrapport kon niet worden gepubliceerd") from error
    return OrdinaryHaDryRunResult(
        report_path=report_path,
        report_file_sha256=_sha256_bytes(report_bytes),
        report=report,
    )


__all__ = [
    "ACTIVE_COMPONENT_DIRECTORY",
    "CUSTOM_COMPONENTS_DIRECTORY",
    "LIVE_INSTALLATION_GATES",
    "MQTT_OFFLINE_WEATHER_STATE",
    "MQTT_OFFLINE_WORKER_STATE",
    "NONFUNCTIONAL_CODE_ROLLBACK_VERSIONS",
    "ORDINARY_HA_DEPLOY_MODE",
    "ORDINARY_HA_DRY_RUN_CONTRACT",
    "OrdinaryHaDryRunError",
    "OrdinaryHaDryRunResult",
    "REQUIRED_CONFIG_ENTRY_VERSION",
    "REQUIRED_RUNTIME_MODE",
    "execute_ordinary_ha_integration_only_dry_run",
    "verify_ordinary_ha_target_layout_contract",
]
