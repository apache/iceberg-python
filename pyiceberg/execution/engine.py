# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Engine resolution: detect and select execution backends.

Follows explicit-over-implicit principle:
1. Per-call override (highest priority)
2. Config file (.pyiceberg.yaml: execution.compute-backend)
3. Environment variable (PYICEBERG_EXECUTION__COMPUTE_BACKEND)
4. Auto-detection (lowest priority -- only promotes DataFusion)

Auto-detection only promotes DataFusion because it is installed explicitly
via `pip install 'pyiceberg[datafusion]'`.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from enum import Enum, auto
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from pyiceberg.execution.protocol import DEFAULT_MEMORY_LIMIT

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyiceberg.execution.protocol import Backends
    from pyiceberg.typedef import Properties


class ExecutionEngine(Enum):
    """Available execution backends.

    Each variant corresponds to an entry in the backend registries
    (_READ_BACKEND_REGISTRY, _COMPUTE_BACKEND_REGISTRY).
    To add a new backend: add an enum value here, add a registry entry,
    and add an availability probe in _detect_available_engines().
    """

    PYARROW = auto()
    DATAFUSION = auto()


@dataclass(frozen=True)
class ResolvedBackends:
    """Result of engine resolution: independently selected read, write, and compute backends."""

    read: ExecutionEngine
    write: ExecutionEngine
    compute: ExecutionEngine


# Operations that may OOM without a spill-capable engine. Used by
# _auto_detect_compute() to emit a UserWarning when PyArrow is the only
# available backend for these operations (informational, not gating).
COMPUTE_INTENSIVE_OPERATIONS = frozenset(
    {
        "cow_rewrite",
        "equality_delete_resolution",
        "sort_on_write",
    }
)


@lru_cache(maxsize=1)
def _detect_available_engines() -> frozenset[ExecutionEngine]:
    """Probe which engines are importable. Cached for process lifetime."""
    available: set[ExecutionEngine] = {ExecutionEngine.PYARROW}  # Always available

    try:
        import datafusion  # noqa: F401

        available.add(ExecutionEngine.DATAFUSION)
    except ImportError:
        pass

    return frozenset(available)


@lru_cache(maxsize=1)
def _read_execution_section_from_file() -> dict[str, str]:
    """Read the full execution section from .pyiceberg.yaml. Cached for process lifetime."""
    from pyiceberg.utils.config import Config

    config = Config()
    exec_section = config.config.get("execution")
    if isinstance(exec_section, dict):
        return {str(k): str(v) for k, v in exec_section.items()}
    return {}


def _read_execution_config_from_file() -> tuple[str | None, str | None, str | None, str | None]:
    """Read execution backend configuration from .pyiceberg.yaml only (no env vars)."""
    section = _read_execution_section_from_file()
    return (
        section.get("compute-backend"),
        section.get("read-backend"),
        section.get("write-backend"),
        section.get("auto-detect"),
    )


def _read_execution_config() -> tuple[str | None, str | None, str | None, str | None]:
    """Read execution backend configuration from config file + env vars."""
    import os

    file_compute, file_read, file_write, file_auto_detect = _read_execution_config_from_file()

    env_compute = os.environ.get("PYICEBERG_EXECUTION__COMPUTE_BACKEND")
    env_read = os.environ.get("PYICEBERG_EXECUTION__READ_BACKEND")
    env_auto_detect = os.environ.get("PYICEBERG_EXECUTION__AUTO_DETECT")

    return (
        env_compute or file_compute,
        env_read or file_read,
        file_write,  # No env var for write (always pyarrow)
        env_auto_detect or file_auto_detect,
    )


def resolve_backends(
    operation: str,
    *,
    read_override: str | None = None,
    write_override: str | None = None,
    compute_override: str | None = None,
) -> ResolvedBackends:
    """Resolve the read, write, and compute engines for an operation.

    Args:
        operation: Name of the operation requesting backends (e.g., "scan", "cow_rewrite").
        read_override: Explicit read backend name (overrides config/auto-detect).
        write_override: Explicit write backend name (overrides config/auto-detect).
        compute_override: Explicit compute backend name (overrides config/auto-detect).

    Returns:
        ResolvedBackends with independently selected engines for each axis.

    Raises:
        ValueError: If an override name is not recognized.
        ImportError: If the named backend is not installed.
    """
    available = _detect_available_engines()

    config_compute, config_read, config_write, config_auto_detect = _read_execution_config()

    effective_compute = compute_override or config_compute
    effective_read = read_override or config_read
    effective_write = write_override or config_write

    # Read backend resolution
    if effective_read:
        read_str = str(effective_read).lower()
        if read_str == "datafusion":
            raise ValueError(
                "read-backend: 'datafusion' is not accepted. The DataFusion read backend is "
                "experimental (materializes full file results in memory, offering no streaming "
                "advantage over PyArrow). If you understand the limitations, use "
                "'datafusion-experimental' to opt in explicitly. "
                "See: https://github.com/apache/datafusion-python/issues/1624"
            )
        elif read_str == "datafusion-experimental":
            if ExecutionEngine.DATAFUSION not in available:
                raise ImportError(
                    "Backend 'datafusion-experimental' requires DataFusion. Install it with: pip install 'pyiceberg[datafusion]'"
                )
            read_engine = ExecutionEngine.DATAFUSION
            warnings.warn(
                "DataFusion read backend is experimental: it materializes full file results "
                "in memory (O(file_size)), offering no streaming advantage over PyArrow. "
                "This will be resolved when datafusion-python supports per-session object "
                "store configuration (https://github.com/apache/datafusion-python/issues/1624). "
                "The default read backend (PyArrow) is recommended for production use.",
                UserWarning,
                stacklevel=2,
            )
        else:
            read_engine = _resolve_explicit(read_str, available, "read")
    else:
        read_engine = ExecutionEngine.PYARROW

    # Write backend resolution
    if effective_write:
        write_engine = _resolve_explicit(str(effective_write), available, "write")
    else:
        write_engine = ExecutionEngine.PYARROW

    # Compute backend resolution
    if effective_compute:
        compute_engine = _resolve_explicit(str(effective_compute), available, "compute")
    else:
        # Check if auto-detect is disabled
        auto_detect_enabled = True
        if config_auto_detect is not None:
            auto_detect_enabled = str(config_auto_detect).lower() in ("1", "true", "yes", "on")

        if auto_detect_enabled:
            compute_engine = _auto_detect_compute(operation, available)
        else:
            compute_engine = ExecutionEngine.PYARROW

    return ResolvedBackends(compute=compute_engine, read=read_engine, write=write_engine)


def _resolve_explicit(choice: str, available: frozenset[ExecutionEngine], role: str) -> ExecutionEngine:
    """Resolve an explicit backend choice string to an ExecutionEngine."""
    mapping = {e.name.lower(): e for e in ExecutionEngine}
    engine = mapping.get(choice.lower())
    if engine is None:
        raise ValueError(f"Unknown {role} backend: '{choice}'. Options: {', '.join(sorted(mapping.keys()))}")
    if engine not in available:
        raise ImportError(f"Backend '{choice}' is not installed. Install it with: pip install {_install_hint(engine)}")
    return engine


def _auto_detect_compute(operation: str, available: frozenset[ExecutionEngine]) -> ExecutionEngine:
    """Auto-detect the best compute backend. Promotes DataFusion if installed."""
    if ExecutionEngine.DATAFUSION in available:
        return ExecutionEngine.DATAFUSION

    # Fallback to PyArrow
    if operation in COMPUTE_INTENSIVE_OPERATIONS:
        warnings.warn(
            f"'{operation}' will use PyArrow (in-memory only, may OOM on large data). "
            f"For bounded-memory execution: pip install 'pyiceberg[datafusion]'",
            UserWarning,
            stacklevel=3,
        )
    return ExecutionEngine.PYARROW


def _install_hint(engine: ExecutionEngine) -> str:
    """Return the pip install command for an engine."""
    hints = {
        ExecutionEngine.DATAFUSION: "'pyiceberg[datafusion]'",
        ExecutionEngine.PYARROW: "pyarrow",
    }
    return hints.get(engine, str(engine.name.lower()))


def clear_config_cache() -> None:
    """Clear all cached engine detection and config resolution state."""
    _detect_available_engines.cache_clear()
    _read_execution_section_from_file.cache_clear()


# =============================================================================
# Backend Instantiation Registry
# =============================================================================

# Declarative mapping from ExecutionEngine → (module_path, class_name) for each axis.
# Adding a new backend requires a single entry here.

_READ_BACKEND_REGISTRY: dict[str, tuple[str, str]] = {
    "PYARROW": ("pyiceberg.execution.backends.pyarrow_backend", "PyArrowReadBackend"),
    "DATAFUSION": ("pyiceberg.execution.backends.datafusion_backend", "DataFusionReadBackend"),
}

_COMPUTE_BACKEND_REGISTRY: dict[str, tuple[str, str]] = {
    "PYARROW": ("pyiceberg.execution.backends.pyarrow_backend", "PyArrowComputeBackend"),
    "DATAFUSION": ("pyiceberg.execution.backends.datafusion_backend", "DataFusionComputeBackend"),
}

_WRITE_BACKEND_REGISTRY: dict[str, tuple[str, str]] = {
    "PYARROW": ("pyiceberg.execution.backends.pyarrow_backend", "PyArrowWriteBackend"),
    # TODO(datafusion#23472, datafusion-python#1637): Add "DATAFUSION" entry once
    # datafusion-python exposes per-file ParquetSink FileMetaData (column statistics).
    # Blocked by: https://github.com/apache/datafusion/issues/23472
    #             https://github.com/apache/datafusion-python/issues/1637
}


def _instantiate_from_registry(
    registry: dict[str, tuple[str, str]],
    engine: ExecutionEngine,
    role: str,
) -> object:
    """Instantiate a backend class from the registry via lazy import."""
    import importlib

    key = engine.name if hasattr(engine, "name") else str(engine)
    entry = registry.get(key)

    if entry is None:
        logger.warning(
            "No registry entry for %s backend '%s'. Falling back to PyArrow. "
            "This may indicate a missing registry entry for a new ExecutionEngine variant.",
            role,
            key,
        )
        entry = registry["PYARROW"]

    module_path, class_name = entry
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(
            f"Cannot instantiate {role} backend '{class_name}' from '{module_path}'. "
            f"Package not installed. Install it with: pip install 'pyiceberg[{key.lower()}]'"
        ) from e

    cls = getattr(module, class_name)
    return cls()


def _instantiate_read(engine: ExecutionEngine) -> object:
    """Instantiate a ReadBackend from an ExecutionEngine enum value."""
    return _instantiate_from_registry(_READ_BACKEND_REGISTRY, engine, "read")


def _instantiate_write(engine: ExecutionEngine = ExecutionEngine.PYARROW) -> object:
    """Instantiate a WriteBackend from an ExecutionEngine enum value."""
    return _instantiate_from_registry(_WRITE_BACKEND_REGISTRY, engine, "write")


def _instantiate_compute(engine: ExecutionEngine) -> object:
    """Instantiate a ComputeBackend from an ExecutionEngine enum value."""
    return _instantiate_from_registry(_COMPUTE_BACKEND_REGISTRY, engine, "compute")


# =============================================================================
# Public Factory: build_backends()
# =============================================================================


def build_backends(io_properties: Properties, operation: str = "scan", **overrides: Any) -> Backends:
    """Build a fully resolved Backends instance from properties and optional overrides.

    Raises:
        TypeError: If an override instance does not satisfy its Protocol.
        ValueError: If a string override name is not recognized.
        ImportError: If a string override names an uninstalled backend.

    Examples:
        >>> from pyiceberg.execution.engine import build_backends
        >>> backends = build_backends({}, compute="pyarrow")
        >>> backends.compute.supports_bounded_memory
        False
    """
    from pyiceberg.execution.protocol import Backends, ComputeBackend, ReadBackend, WriteBackend

    read_override = overrides.get("read")
    write_override = overrides.get("write")
    compute_override = overrides.get("compute")

    all_instances = (
        read_override is not None
        and not isinstance(read_override, str)
        and write_override is not None
        and not isinstance(write_override, str)
        and compute_override is not None
        and not isinstance(compute_override, str)
    )

    if all_instances:
        read, write, compute = read_override, write_override, compute_override
    else:
        resolved = resolve_backends(
            operation,
            read_override=read_override if isinstance(read_override, str) else None,
            write_override=write_override if isinstance(write_override, str) else None,
            compute_override=compute_override if isinstance(compute_override, str) else None,
        )

        read = read_override if not isinstance(read_override, (str, type(None))) else _instantiate_read(resolved.read)
        write = write_override if not isinstance(write_override, (str, type(None))) else _instantiate_write(resolved.write)
        compute = (
            compute_override if not isinstance(compute_override, (str, type(None))) else _instantiate_compute(resolved.compute)
        )

    if not isinstance(read, ReadBackend):
        raise TypeError(
            f"Resolved read backend does not satisfy ReadBackend protocol: {type(read).__name__}. "
            f"It must implement read_parquet()."
        )
    if not isinstance(write, WriteBackend):
        raise TypeError(
            f"Resolved write backend does not satisfy WriteBackend protocol: {type(write).__name__}. "
            f"It must implement write_data_file()."
        )
    if not isinstance(compute, ComputeBackend):
        raise TypeError(
            f"Resolved compute backend does not satisfy ComputeBackend protocol: {type(compute).__name__}. "
            f"It must implement filter(), sort_from_files(), anti_join_from_files(), "
            f"and apply_positional_deletes()."
        )

    import types

    frozen_props = types.MappingProxyType(dict(io_properties))

    return Backends(read=read, write=write, compute=compute, io_properties=frozen_props)


# =============================================================================
# Execution Config Helpers
# =============================================================================


def get_execution_config_int(key: str, default: int) -> int:
    """Read an integer from the execution config section.

    Checks env var PYICEBERG_EXECUTION__<KEY> first, then .pyiceberg.yaml.
    """
    import os

    env_key = f"PYICEBERG_EXECUTION__{key.upper().replace('-', '_')}"
    env_val = os.environ.get(env_key)
    if env_val is not None:
        try:
            return int(env_val)
        except (ValueError, TypeError):
            pass

    section = _read_execution_section_from_file()
    val = section.get(key)
    if val is not None:
        try:
            return int(val)
        except (ValueError, TypeError):
            pass

    return default


def get_memory_limit() -> int:
    """Read the memory limit for compute operations from config, env var, or default (512 MB).

    Resolution priority (highest to lowest):
        1. Environment variable: PYICEBERG_EXECUTION__MEMORY_LIMIT
        2. Config file (.pyiceberg.yaml): execution.memory-limit
        3. Default: 536870912 (512 MB)

    Returns:
        Memory limit in bytes.
    """
    return get_execution_config_int("memory-limit", DEFAULT_MEMORY_LIMIT)


# =============================================================================
# Execution Threshold Defaults
# =============================================================================

#: Default threshold (2 GB) above which a ResourceWarning is emitted when
#: calling to_arrow(). Suggests using to_arrow_batch_reader() for streaming.
#: Configurable via execution.oom-warning-threshold in .pyiceberg.yaml (bytes)
#: or PYICEBERG_EXECUTION__OOM_WARNING_THRESHOLD env var.
OOM_WARNING_THRESHOLD_BYTES: int = 2 * 1024 * 1024 * 1024

#: CoW delete: files below this compressed size use single-pass materialization
#: (one read, O(file_size) memory). Files at or above use two-pass streaming
#: (two reads, O(batch_size) memory). 64 MB compressed ≈ 320 MB in Arrow typical case.
#: Configurable via execution.cow-threshold in .pyiceberg.yaml (value in bytes)
#: or PYICEBERG_EXECUTION__COW_THRESHOLD env var.
COW_THRESHOLD_DEFAULT: int = 64 * 1024 * 1024

#: Delete file count above which bounded-memory planning is used (if DataFusion available).
#: Configurable via execution.planning-threshold in .pyiceberg.yaml
#: or PYICEBERG_EXECUTION__PLANNING_THRESHOLD env var.
BOUNDED_PLANNER_THRESHOLD: int = 100_000
