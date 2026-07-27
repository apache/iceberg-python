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

"""Tests for execution engine configuration, registry, thread safety, and memory limits.

Covers:
- Backend configuration via .pyiceberg.yaml and env vars
- Registry pattern (OCP compliance, declarative entries, lazy imports)
- Thread safety documentation for _schema_cache
- Public module __all__ declarations
- Memory limit configuration wiring
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pyiceberg.execution.engine import ExecutionEngine, resolve_backends

# =============================================================================
# From test_config.py
# =============================================================================


class TestBackendsIoPropertiesField:
    """Verify io_properties is a proper declared dataclass field on Backends.

    Regression test for the monkey-patching fix:
    BEFORE: instance._io_properties = io_properties  # type: ignore[attr-defined]
    AFTER:  io_properties is a declared @dataclass field, passed via constructor.
    """

    def test_io_properties_is_declared_dataclass_field(self) -> None:
        """Backends.io_properties must be a declared field, not a monkey-patched attribute."""
        import dataclasses

        from pyiceberg.execution.protocol import Backends

        field_names = [f.name for f in dataclasses.fields(Backends)]
        assert "io_properties" in field_names, (
            "io_properties is not a declared dataclass field on Backends. "
            "It should not be set via monkey-patching (instance._io_properties = ...)."
        )

    def test_io_properties_accessible_without_type_ignore(self) -> None:
        """Accessing backends.io_properties must not require type: ignore suppression."""
        from pyiceberg.execution.protocol import (
            Backends,
            ComputeBackend,
            ReadBackend,
            WriteBackend,
        )

        # Construct directly (bypass resolve which needs Config/strictyaml)
        mock_read = MagicMock(spec=ReadBackend)
        mock_write = MagicMock(spec=WriteBackend)
        mock_compute = MagicMock(spec=ComputeBackend)
        props = {"s3.access-key-id": "AKIA_TEST", "s3.region": "us-east-1"}

        backends = Backends(read=mock_read, write=mock_write, compute=mock_compute, io_properties=props)

        # Access as a normal attribute -- no underscore prefix, no type: ignore needed
        assert backends.io_properties is props
        assert backends.io_properties["s3.access-key-id"] == "AKIA_TEST"

    def test_io_properties_passed_through_resolve(self) -> None:
        """Backends.resolve(io_properties) must store io_properties values on the returned instance."""
        from pyiceberg.execution.protocol import Backends

        props = {"warehouse": "s3://my-bucket/tables", "s3.region": "eu-west-1"}

        with patch("pyiceberg.execution.engine.resolve_backends") as mock_resolve_engine:
            mock_resolve_engine.return_value = MagicMock(read=MagicMock(), write=MagicMock(), compute=MagicMock())
            backends = Backends.resolve(props)

        # io_properties is a frozen snapshot (MappingProxyType) with the same values.
        # It is NOT the same object (identity) -- that's intentional for credential safety.
        assert dict(backends.io_properties) == props

    def test_io_properties_equality_in_dataclass(self) -> None:
        """Two Backends instances with different io_properties must not be equal."""
        from pyiceberg.execution.protocol import Backends

        mock_read = MagicMock()
        mock_write = MagicMock()
        mock_compute = MagicMock()

        b1 = Backends(read=mock_read, write=mock_write, compute=mock_compute, io_properties={"region": "us"})
        b2 = Backends(read=mock_read, write=mock_write, compute=mock_compute, io_properties={"region": "eu"})

        assert b1 != b2, "Backends with different io_properties should not be equal"


class TestBackendsResolveValidation:
    """Verify Backends.resolve fails fast with a clear error on invalid overrides.

    Tests for the fail-fast validation added to Backends.resolve().
    Invalid backend instances that don't satisfy the protocol should raise
    TypeError at resolve time, not produce cryptic AttributeErrors later.
    """

    def test_invalid_read_override_raises_type_error(self) -> None:
        """Passing an object that doesn't satisfy ReadBackend raises TypeError."""
        from pyiceberg.execution.protocol import Backends

        class NotAReadBackend:
            pass

        with patch("pyiceberg.execution.engine.resolve_backends") as mock_resolve:
            mock_resolve.return_value = MagicMock(read=MagicMock(), write=MagicMock(), compute=MagicMock())
            with pytest.raises(TypeError, match="ReadBackend protocol"):
                Backends.resolve({}, read=NotAReadBackend())

    def test_write_string_override_resolves_pyarrow(self) -> None:
        """Passing write='pyarrow' resolves to PyArrowWriteBackend via registry."""
        from pyiceberg.execution.protocol import Backends

        backends = Backends.resolve({}, write="pyarrow")
        assert type(backends.write).__name__ == "PyArrowWriteBackend"

    def test_write_string_override_rejects_unknown(self) -> None:
        """Passing an unknown write backend string raises ValueError."""
        from pyiceberg.execution.protocol import Backends

        with pytest.raises((ValueError, ImportError)):
            Backends.resolve({}, write="nonexistent")

    def test_invalid_write_override_raises_type_error(self) -> None:
        """Passing an object that doesn't satisfy WriteBackend raises TypeError."""
        from pyiceberg.execution.protocol import Backends

        class NotAWriteBackend:
            pass

        with patch("pyiceberg.execution.engine.resolve_backends") as mock_resolve:
            mock_resolve.return_value = MagicMock(read=MagicMock(), write=MagicMock(), compute=MagicMock())
            with pytest.raises(TypeError, match="WriteBackend protocol"):
                Backends.resolve({}, write=NotAWriteBackend())

    def test_invalid_compute_override_raises_type_error(self) -> None:
        """Passing an object that doesn't satisfy ComputeBackend raises TypeError."""
        from pyiceberg.execution.protocol import Backends

        class NotAComputeBackend:
            pass

        with patch("pyiceberg.execution.engine.resolve_backends") as mock_resolve:
            mock_resolve.return_value = MagicMock(read=MagicMock(), write=MagicMock(), compute=MagicMock())
            with pytest.raises(TypeError, match="ComputeBackend protocol"):
                Backends.resolve({}, compute=NotAComputeBackend())

    def test_valid_overrides_do_not_raise(self) -> None:
        """Valid protocol-compliant overrides pass validation without error."""
        from pyiceberg.execution.backends.pyarrow_backend import (
            PyArrowComputeBackend,
            PyArrowReadBackend,
            PyArrowWriteBackend,
        )
        from pyiceberg.execution.protocol import Backends

        with patch("pyiceberg.execution.engine.resolve_backends") as mock_resolve:
            mock_resolve.return_value = MagicMock(read=MagicMock(), write=MagicMock(), compute=MagicMock())
            # Should not raise
            result = Backends.resolve(
                {},
                read=PyArrowReadBackend(),
                write=PyArrowWriteBackend(),
                compute=PyArrowComputeBackend(),
            )
        assert result is not None

    def test_error_message_includes_missing_methods(self) -> None:
        """Error message should hint at which methods are needed."""
        from pyiceberg.execution.protocol import Backends

        class EmptyClass:
            pass

        with patch("pyiceberg.execution.engine.resolve_backends") as mock_resolve:
            mock_resolve.return_value = MagicMock(read=MagicMock(), write=MagicMock(), compute=MagicMock())
            with pytest.raises(TypeError, match="read_parquet"):
                Backends.resolve({}, read=EmptyClass())


class TestConfigResolution:
    """Verify resolve_backends reads from Config() for backend selection."""

    def test_env_var_sets_compute_backend(self) -> None:
        """PYICEBERG_EXECUTION__COMPUTE_BACKEND env var should override auto-detection."""
        # Force pyarrow via env var
        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__COMPUTE_BACKEND": "pyarrow"}, clear=False):
            resolved = resolve_backends("test_op")
        assert resolved.compute == ExecutionEngine.PYARROW

    def test_env_var_invalid_backend_raises(self) -> None:
        """Invalid backend name in env var should raise ValueError or ImportError."""
        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__COMPUTE_BACKEND": "nonexistent"}, clear=False):
            with pytest.raises((ValueError, ImportError)):
                resolve_backends("test_op")

    def test_explicit_override_beats_env_var(self) -> None:
        """Per-call override should take precedence over env var."""
        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__COMPUTE_BACKEND": "pyarrow"}, clear=False):
            # Even though env says pyarrow, explicit override says pyarrow too (same result)
            resolved = resolve_backends("test_op", compute_override="pyarrow")
        assert resolved.compute == ExecutionEngine.PYARROW

    def test_auto_detect_disabled_via_env(self) -> None:
        """PYICEBERG_EXECUTION__AUTO_DETECT=false should disable auto-promotion."""
        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__AUTO_DETECT": "false"}, clear=False):
            resolved = resolve_backends("test_op")
        # Should use PyArrow regardless of what's installed
        assert resolved.compute == ExecutionEngine.PYARROW


class TestInstantiateWriteAlwaysPyArrow:
    """Verify _instantiate_write takes no parameters and always returns PyArrowWriteBackend.

    The write backend is always PyArrow because it's the only backend that produces
    the detailed Parquet file statistics (column sizes, null counts, split offsets)
    required for Iceberg DataFile manifest entries.
    """

    def test_instantiate_write_takes_no_parameters(self) -> None:
        """_instantiate_write must be callable with zero arguments."""
        from pyiceberg.execution.engine import _instantiate_write

        sig = inspect.signature(_instantiate_write)
        params = [p for p in sig.parameters.values() if p.default is inspect.Parameter.empty]
        assert len(params) == 0, f"_instantiate_write should take no required parameters, but has: {[p.name for p in params]}"

    def test_instantiate_write_returns_pyarrow_write_backend(self) -> None:
        """_instantiate_write must return a PyArrowWriteBackend instance."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        from pyiceberg.execution.engine import _instantiate_write

        result = _instantiate_write()
        assert isinstance(result, PyArrowWriteBackend)

    def test_backends_resolve_always_produces_pyarrow_write(self) -> None:
        """Backends.resolve() must always use PyArrowWriteBackend regardless of compute engine."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        from pyiceberg.execution.protocol import Backends

        backends = Backends.resolve({})
        assert isinstance(backends.write, PyArrowWriteBackend), (
            f"Write backend should always be PyArrowWriteBackend, got {type(backends.write).__name__}"
        )


class TestScopedEnvVarsSerializationWarning:
    """Verify _scoped_env_vars behavior with and without the fast-path optimization.

    The fast-path: when env vars are already set to the correct values, no lock
    is acquired and no warning is needed (parallel execution is not affected).
    The slow path (env vars need to change) acquires the lock -- parallelism is
    limited only during the env mutation, not the full operation.
    """

    def test_no_warning_on_fast_path(self) -> None:
        """When env vars are already correct, no warning is emitted (fast path)."""
        import warnings

        from pyiceberg.execution.object_store import _scoped_env_vars

        os.environ["AWS_ACCESS_KEY_ID"] = "test_key"
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                with _scoped_env_vars({"AWS_ACCESS_KEY_ID": "test_key"}):
                    pass

            user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
            assert len(user_warnings) == 0, "No warning on fast path (env already correct)"
        finally:
            os.environ.pop("AWS_ACCESS_KEY_ID", None)

    def test_no_warning_when_env_map_is_empty(self) -> None:
        """Empty env_map (local files) does NOT emit any warning."""
        import warnings

        from pyiceberg.execution.object_store import _scoped_env_vars

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with _scoped_env_vars({}):
                pass

        user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
        assert len(user_warnings) == 0, "No warning expected for empty env map"

    def test_env_vars_restored_after_scoped_block(self) -> None:
        """Environment variables are fully restored after the context manager exits."""
        import warnings

        from pyiceberg.execution.object_store import _scoped_env_vars

        os.environ["__TEST_PYICEBERG_KEY"] = "original"

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with _scoped_env_vars({"__TEST_PYICEBERG_KEY": "modified", "__TEST_PYICEBERG_NEW": "new_val"}):
                    assert os.environ["__TEST_PYICEBERG_KEY"] == "modified"
                    assert os.environ["__TEST_PYICEBERG_NEW"] == "new_val"

            assert os.environ["__TEST_PYICEBERG_KEY"] == "original"
            assert "__TEST_PYICEBERG_NEW" not in os.environ
        finally:
            os.environ.pop("__TEST_PYICEBERG_KEY", None)

    def test_env_vars_restored_on_exception(self) -> None:
        """Environment variables are restored even when an exception occurs inside the block."""
        import warnings

        from pyiceberg.execution.object_store import _scoped_env_vars

        os.environ["__TEST_PYICEBERG_EXC"] = "before"

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    with _scoped_env_vars({"__TEST_PYICEBERG_EXC": "during"}):
                        assert os.environ["__TEST_PYICEBERG_EXC"] == "during"
                        raise RuntimeError("simulated failure")
                except RuntimeError:
                    pass

            assert os.environ["__TEST_PYICEBERG_EXC"] == "before"
        finally:
            os.environ.pop("__TEST_PYICEBERG_EXC", None)

    def test_todo_comment_references_issue_1624(self) -> None:
        """object_store.py must reference #1624 as the long-term fix for removing the lock."""
        import pyiceberg.execution.object_store as obj_store

        source = inspect.getsource(obj_store)
        assert "1624" in source, (
            "object_store.py must reference datafusion-python issue #1624 as the TODO for removing the env var lock mechanism."
        )

    def test_no_global_mutable_state(self) -> None:
        """object_store.py must NOT use global mutable state for warning deduplication."""
        import pyiceberg.execution.object_store as mod

        assert not hasattr(mod, "_SERIALIZATION_WARNING_EMITTED"), (
            "object_store.py still uses _SERIALIZATION_WARNING_EMITTED global state. "
            "Use Python's built-in warnings deduplication instead."
        )
        assert not hasattr(mod, "_reset_serialization_warning"), (
            "object_store.py still has _reset_serialization_warning. "
            "No manual reset is needed when using Python's warnings module."
        )


# =============================================================================
# From test_registry.py
# =============================================================================


class TestRegistryDeclarative:
    """Registry entries are declarative tuples -- no logic, just data."""

    def test_read_registry_is_dict_of_tuples(self) -> None:
        """_READ_BACKEND_REGISTRY must be a dict mapping str → (module_path, class_name)."""
        from pyiceberg.execution.engine import _READ_BACKEND_REGISTRY

        assert isinstance(_READ_BACKEND_REGISTRY, dict)
        for key, value in _READ_BACKEND_REGISTRY.items():
            assert isinstance(key, str), f"Registry key must be str, got {type(key)}"
            assert isinstance(value, tuple), f"Registry value must be tuple, got {type(value)}"
            assert len(value) == 2, f"Registry tuple must have 2 elements (module, class), got {len(value)}"
            module_path, class_name = value
            assert isinstance(module_path, str)
            assert isinstance(class_name, str)
            assert "." in module_path, f"Module path should be dotted: {module_path}"

    def test_compute_registry_is_dict_of_tuples(self) -> None:
        """_COMPUTE_BACKEND_REGISTRY must be a dict mapping str → (module_path, class_name)."""
        from pyiceberg.execution.engine import _COMPUTE_BACKEND_REGISTRY

        assert isinstance(_COMPUTE_BACKEND_REGISTRY, dict)
        for key, value in _COMPUTE_BACKEND_REGISTRY.items():
            assert isinstance(key, str)
            assert isinstance(value, tuple)
            assert len(value) == 2

    def test_write_backend_registry_has_pyarrow(self) -> None:
        """_WRITE_BACKEND_REGISTRY must have a PYARROW entry as (module_path, class_name)."""
        from pyiceberg.execution.engine import _WRITE_BACKEND_REGISTRY

        assert "PYARROW" in _WRITE_BACKEND_REGISTRY
        entry = _WRITE_BACKEND_REGISTRY["PYARROW"]
        assert isinstance(entry, tuple)
        assert len(entry) == 2
        module_path, class_name = entry
        assert "pyarrow" in module_path.lower()
        assert "Write" in class_name


class TestRegistryConsistencyWithEnum:
    """Registry keys must correspond to ExecutionEngine variant names."""

    def test_read_registry_keys_match_enum_names(self) -> None:
        """Every key in _READ_BACKEND_REGISTRY must be a valid ExecutionEngine.name."""
        from pyiceberg.execution.engine import _READ_BACKEND_REGISTRY

        valid_names = {e.name for e in ExecutionEngine}
        for key in _READ_BACKEND_REGISTRY:
            assert key in valid_names, f"Registry key '{key}' is not a valid ExecutionEngine name. Valid: {sorted(valid_names)}"

    def test_compute_registry_keys_match_enum_names(self) -> None:
        """Every key in _COMPUTE_BACKEND_REGISTRY must be a valid ExecutionEngine.name."""
        from pyiceberg.execution.engine import _COMPUTE_BACKEND_REGISTRY

        valid_names = {e.name for e in ExecutionEngine}
        for key in _COMPUTE_BACKEND_REGISTRY:
            assert key in valid_names, f"Registry key '{key}' is not a valid ExecutionEngine name. Valid: {sorted(valid_names)}"

    def test_pyarrow_always_in_both_registries(self) -> None:
        """PYARROW must always be registered (it is the mandatory fallback)."""
        from pyiceberg.execution.engine import (
            _COMPUTE_BACKEND_REGISTRY,
            _READ_BACKEND_REGISTRY,
        )

        assert "PYARROW" in _READ_BACKEND_REGISTRY, "PYARROW must be in _READ_BACKEND_REGISTRY (fallback)"
        assert "PYARROW" in _COMPUTE_BACKEND_REGISTRY, "PYARROW must be in _COMPUTE_BACKEND_REGISTRY (fallback)"

    def test_every_engine_has_at_least_read_and_compute_entries(self) -> None:
        """Every ExecutionEngine variant should have entries in both registries."""
        from pyiceberg.execution.engine import (
            _COMPUTE_BACKEND_REGISTRY,
            _READ_BACKEND_REGISTRY,
        )

        for engine in ExecutionEngine:
            assert engine.name in _READ_BACKEND_REGISTRY, f"ExecutionEngine.{engine.name} has no entry in _READ_BACKEND_REGISTRY"
            assert engine.name in _COMPUTE_BACKEND_REGISTRY, (
                f"ExecutionEngine.{engine.name} has no entry in _COMPUTE_BACKEND_REGISTRY"
            )


class TestRegistryInstantiation:
    """_instantiate_from_registry correctly resolves and imports backends."""

    def test_pyarrow_read_instantiates_correctly(self) -> None:
        """Passing ExecutionEngine.PYARROW to _instantiate_read returns PyArrowReadBackend."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowReadBackend
        from pyiceberg.execution.engine import _instantiate_read

        result = _instantiate_read(ExecutionEngine.PYARROW)
        assert isinstance(result, PyArrowReadBackend)

    def test_pyarrow_compute_instantiates_correctly(self) -> None:
        """Passing ExecutionEngine.PYARROW to _instantiate_compute returns PyArrowComputeBackend."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
        from pyiceberg.execution.engine import _instantiate_compute

        result = _instantiate_compute(ExecutionEngine.PYARROW)
        assert isinstance(result, PyArrowComputeBackend)

    def test_datafusion_read_instantiates_when_available(self) -> None:
        """Passing ExecutionEngine.DATAFUSION returns DataFusionReadBackend if installed."""
        pytest.importorskip("datafusion")
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionReadBackend,
        )
        from pyiceberg.execution.engine import _instantiate_read

        result = _instantiate_read(ExecutionEngine.DATAFUSION)
        assert isinstance(result, DataFusionReadBackend)

    def test_datafusion_compute_instantiates_when_available(self) -> None:
        """Passing ExecutionEngine.DATAFUSION returns DataFusionComputeBackend if installed."""
        pytest.importorskip("datafusion")
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.execution.engine import _instantiate_compute

        result = _instantiate_compute(ExecutionEngine.DATAFUSION)
        assert isinstance(result, DataFusionComputeBackend)

    def test_unknown_engine_falls_back_to_pyarrow(self) -> None:
        """An engine name not in the registry falls back to PyArrow."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
        from pyiceberg.execution.engine import (
            _COMPUTE_BACKEND_REGISTRY,
            _instantiate_from_registry,
        )

        fake_engine = MagicMock()
        fake_engine.name = "UNKNOWN_ENGINE"

        result = _instantiate_from_registry(_COMPUTE_BACKEND_REGISTRY, fake_engine, "compute")
        assert isinstance(result, PyArrowComputeBackend)

    def test_missing_package_raises_import_error_with_hint(self) -> None:
        """If the backend's package isn't installed, ImportError includes install hint."""
        from pyiceberg.execution.engine import _instantiate_from_registry

        # Create a registry entry pointing to a non-existent module
        fake_registry = {
            "PYARROW": ("pyiceberg.execution.backends.pyarrow_backend", "PyArrowReadBackend"),
            "FAKE": ("pyiceberg.execution.backends.nonexistent_backend", "FakeBackend"),
        }
        fake_engine = MagicMock()
        fake_engine.name = "FAKE"

        with pytest.raises(ImportError, match="pip install"):
            _instantiate_from_registry(fake_registry, fake_engine, "read")


class TestRegistryLazyImport:
    """Backend modules are NOT imported at registry definition time."""

    def test_importing_protocol_does_not_import_datafusion(self) -> None:
        """Importing pyiceberg.execution.protocol must not trigger datafusion import.

        The registry stores strings (module paths), not actual modules. This ensures
        that `import pyiceberg.execution.protocol` is fast and does not fail when
        optional backends are not installed.
        """
        import sys

        # Remove datafusion from sys.modules if present (to detect fresh import)
        set(sys.modules.keys())

        # Re-import protocol (may already be cached, but registry access should not trigger)
        from pyiceberg.execution.engine import (
            _COMPUTE_BACKEND_REGISTRY,
            _READ_BACKEND_REGISTRY,
        )

        # Access registry values -- should be strings, not module references
        for _, (module_path, class_name) in _READ_BACKEND_REGISTRY.items():
            assert isinstance(module_path, str)
            assert isinstance(class_name, str)

        for _, (module_path, class_name) in _COMPUTE_BACKEND_REGISTRY.items():
            assert isinstance(module_path, str)
            assert isinstance(class_name, str)

    def test_instantiate_uses_importlib(self) -> None:
        """_instantiate_from_registry must use importlib.import_module for lazy loading."""
        from pyiceberg.execution.engine import _instantiate_from_registry

        source = inspect.getsource(_instantiate_from_registry)
        assert "importlib" in source, "_instantiate_from_registry should use importlib for lazy imports"


class TestResolveExplicitDerivedFromEnum:
    """_resolve_explicit mapping is auto-derived from ExecutionEngine, not hard-coded."""

    def test_all_enum_variants_are_valid_config_strings(self) -> None:
        """Every ExecutionEngine.name.lower() must be accepted by _resolve_explicit."""
        from pyiceberg.execution.engine import (
            _detect_available_engines,
            _resolve_explicit,
        )

        available = _detect_available_engines()
        for engine in ExecutionEngine:
            if engine in available:
                # Should not raise
                result = _resolve_explicit(engine.name.lower(), available, "test")
                assert result == engine

    def test_case_insensitive_resolution(self) -> None:
        """Config strings are case-insensitive (pyarrow, PYARROW, PyArrow all work)."""
        from pyiceberg.execution.engine import (
            _detect_available_engines,
            _resolve_explicit,
        )

        available = _detect_available_engines()
        # PyArrow is always available
        assert _resolve_explicit("pyarrow", available, "test") == ExecutionEngine.PYARROW
        assert _resolve_explicit("PYARROW", available, "test") == ExecutionEngine.PYARROW
        assert _resolve_explicit("PyArrow", available, "test") == ExecutionEngine.PYARROW

    def test_no_hardcoded_mapping_dict(self) -> None:
        """_resolve_explicit should derive mapping from enum, not maintain a separate dict literal."""
        from pyiceberg.execution.engine import _resolve_explicit

        source = inspect.getsource(_resolve_explicit)
        # The mapping should be a comprehension over ExecutionEngine, not a dict literal
        assert "for e in ExecutionEngine" in source or "{e.name" in source, (
            "_resolve_explicit should derive its mapping from the ExecutionEngine enum "
            "to maintain single-source-of-truth. Found what appears to be a hard-coded dict."
        )


class TestRegistryExtensibility:
    """Validate that the registry pattern enables OCP-compliant extension."""

    def test_adding_entry_to_registry_enables_instantiation(self) -> None:
        """A new entry added to the registry at runtime should be instantiable."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
        from pyiceberg.execution.engine import (
            _COMPUTE_BACKEND_REGISTRY,
            _instantiate_from_registry,
        )

        # Simulate adding a new backend that maps to the same PyArrow class (for testing)
        extended_registry = dict(_COMPUTE_BACKEND_REGISTRY)
        extended_registry["SIMULATED"] = ("pyiceberg.execution.backends.pyarrow_backend", "PyArrowComputeBackend")

        fake_engine = MagicMock()
        fake_engine.name = "SIMULATED"

        result = _instantiate_from_registry(extended_registry, fake_engine, "compute")
        assert isinstance(result, PyArrowComputeBackend)

    def test_registry_does_not_require_code_changes_for_new_backend(self) -> None:
        """The _instantiate_from_registry function has no backend-specific logic.

        It should work purely from the registry data without any if/elif branches
        that reference specific engine names.
        """
        from pyiceberg.execution.engine import _instantiate_from_registry

        source = inspect.getsource(_instantiate_from_registry)
        # Should NOT contain backend-specific names
        assert "DATAFUSION" not in source, "Generic function should not reference specific backends"
        assert "DUCKDB" not in source, "Generic function should not reference specific backends"
        assert "POLARS" not in source, "Generic function should not reference specific backends"
        # Should NOT have if/elif chains (only the None fallback check is acceptable)
        elif_count = source.count("elif")
        assert elif_count == 0, (
            f"_instantiate_from_registry has {elif_count} elif branches. "
            f"It should be a generic lookup without backend-specific branching."
        )


# =============================================================================
# From test_thread_safety_and_exports.py
# =============================================================================


class TestSchemaCacheThreadSafetyDocumentation:
    """The schema_cache comment must explain safety via idempotence, not dict atomicity."""

    def test_comment_mentions_idempotent(self) -> None:
        """The comment must use 'idempotent' to explain why concurrent access is safe."""
        from pyiceberg.execution._orchestrate import orchestrate_scan

        source = inspect.getsource(orchestrate_scan)
        # Find the _schema_cache block
        assert "idempotent" in source.lower(), (
            "The schema_cache comment must explain that concurrent access is safe "
            "because the cached computation is idempotent (same input → same output). "
            "Do NOT rely on CPython dict atomicity as the justification."
        )

    def test_comment_does_not_claim_dict_atomicity(self) -> None:
        """The comment must NOT claim Python dicts are 'atomic for distinct keys'.

        This is a CPython implementation detail (GIL makes dict.__setitem__ appear
        atomic) and is NOT a language guarantee. PyPy, GraalPy, and free-threaded
        CPython (PEP 703) do NOT guarantee this.
        """
        from pyiceberg.execution._orchestrate import orchestrate_scan

        source = inspect.getsource(orchestrate_scan)
        # Extract just the schema_cache comment block (nearby lines)
        lines = source.split("\n")
        cache_lines = []
        for i, line in enumerate(lines):
            if "schema_cache" in line and "dict[" in line:
                # Grab the comment block above
                start = max(0, i - 6)
                cache_lines = lines[start : i + 1]
                break

        comment_block = "\n".join(cache_lines)
        assert "atomic for distinct keys" not in comment_block, (
            "The schema_cache comment claims Python dicts are 'atomic for distinct keys'. "
            "This is a CPython implementation detail, not a language guarantee. "
            "Explain safety via idempotence instead."
        )

    def test_comment_mentions_deterministic_or_pure(self) -> None:
        """The comment should explain that pyarrow_to_schema is a pure/deterministic function."""
        from pyiceberg.execution._orchestrate import orchestrate_scan

        source = inspect.getsource(orchestrate_scan)
        lines = source.split("\n")
        cache_lines = []
        for i, line in enumerate(lines):
            if "schema_cache" in line and "dict[" in line:
                start = max(0, i - 8)
                cache_lines = lines[start : i + 1]
                break

        comment_block = "\n".join(cache_lines).lower()
        has_explanation = (
            "deterministic" in comment_block
            or "pure" in comment_block
            or "same input" in comment_block
            or "same schema" in comment_block
            or "idempotent" in comment_block
        )
        assert has_explanation, (
            "The _schema_cache comment must explain that the cached computation "
            "is deterministic/pure (same Arrow schema always produces the same "
            "Iceberg Schema), making redundant computation harmless."
        )


class TestPublicModulesDeclareAll:
    """Public modules in pyiceberg.execution must declare __all__.

    This distinguishes the intended public API from internal helpers that are
    importable cross-module but not meant for external use. Private modules
    (underscore-prefixed: _orchestrate.py, _sorted_reader.py) do NOT need
    __all__ since the underscore prefix signals "internal".
    """

    def test_expression_to_sql_has_all(self) -> None:
        """expression_to_sql.py must declare __all__."""
        import pyiceberg.execution.expression_to_sql as mod

        assert hasattr(mod, "__all__"), (
            "expression_to_sql.py must declare __all__ to distinguish its public API "
            "(expression_to_sql) from internal helpers (_escape_sql_string, etc.)."
        )

    def test_expression_to_sql_all_contains_public_function(self) -> None:
        """expression_to_sql.__all__ must include the public function."""
        from pyiceberg.execution.expression_to_sql import __all__

        assert "expression_to_sql" in __all__

    def test_object_store_has_all(self) -> None:
        """object_store.py must declare __all__."""
        import pyiceberg.execution.object_store as mod

        assert hasattr(mod, "__all__"), (
            "object_store.py must declare __all__ to distinguish its public API "
            "from internal helpers (_scoped_env_vars, _ENV_LOCK, etc.)."
        )

    def test_object_store_all_contains_public_functions(self) -> None:
        """object_store.__all__ must include the public credential-config functions."""
        from pyiceberg.execution.object_store import __all__

        assert "datafusion_env_vars_from_properties" in __all__

    def test_materialize_has_all(self) -> None:
        """materialize.py must declare __all__."""
        import pyiceberg.execution.materialize as mod

        assert hasattr(mod, "__all__"), (
            "materialize.py must declare __all__ to distinguish its public API "
            "from internal state (_active_temp_files, _cleanup_remaining_temp_files)."
        )

    def test_materialize_all_contains_public_functions(self) -> None:
        """materialize.__all__ must include the public context managers."""
        from pyiceberg.execution.materialize import __all__

        assert "materialize_to_parquet" in __all__
        assert "materialize_batches_to_parquet" in __all__

    def test_planning_has_all(self) -> None:
        """planning.py must declare __all__."""
        import pyiceberg.execution.planning as mod

        assert hasattr(mod, "__all__"), (
            "planning.py must declare __all__ to distinguish its public classes from internal helpers (_serialize_partition_key)."
        )

    def test_planning_all_contains_public_classes(self) -> None:
        """planning.__all__ must include the planner implementations."""
        from pyiceberg.execution.planning import __all__

        assert "InMemoryPlanner" in __all__
        assert "BoundedMemoryPlanner" in __all__

    def test_private_modules_do_not_need_all(self) -> None:
        """Private modules (underscore-prefixed) do NOT need __all__."""
        import pyiceberg.execution._orchestrate as mod
        import pyiceberg.execution._sorted_reader as mod2

        # These are internal -- __all__ is optional (underscore prefix signals private)
        # This test just verifies they're importable (no assertion on __all__)
        assert mod is not None
        assert mod2 is not None


# =============================================================================
# From test_memory_limit_config.py
# =============================================================================


class TestGetMemoryLimit:
    """Verify get_memory_limit() reads from env var, config file, and default."""

    def test_function_exists_and_is_importable(self) -> None:
        """get_memory_limit must be importable from pyiceberg.execution.engine."""
        from pyiceberg.execution.engine import get_memory_limit

        assert callable(get_memory_limit)

    def test_returns_default_when_no_config(self) -> None:
        """With no env var and no config, returns DEFAULT_MEMORY_LIMIT (512 MB)."""
        from pyiceberg.execution.engine import get_memory_limit
        from pyiceberg.execution.protocol import DEFAULT_MEMORY_LIMIT

        result = get_memory_limit()
        assert result == DEFAULT_MEMORY_LIMIT
        assert result == 512 * 1024 * 1024

    def test_env_var_overrides_default(self) -> None:
        """PYICEBERG_EXECUTION__MEMORY_LIMIT env var takes highest priority."""
        from pyiceberg.execution.engine import get_memory_limit

        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__MEMORY_LIMIT": "1073741824"}):
            result = get_memory_limit()
        assert result == 1073741824  # 1 GB

    def test_env_var_invalid_falls_through_to_default(self) -> None:
        """Non-integer env var value falls through to config or default."""
        from pyiceberg.execution.engine import get_memory_limit
        from pyiceberg.execution.protocol import DEFAULT_MEMORY_LIMIT

        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__MEMORY_LIMIT": "not_a_number"}):
            result = get_memory_limit()
        assert result == DEFAULT_MEMORY_LIMIT

    def test_config_file_overrides_default(self) -> None:
        """execution.memory-limit in .pyiceberg.yaml overrides default."""
        from pyiceberg.execution.engine import (
            _read_execution_section_from_file,
            get_memory_limit,
        )

        mock_config = {"execution": {"memory-limit": 268435456}}  # 256 MB
        with patch("pyiceberg.utils.config.Config") as MockConfig:
            MockConfig.return_value.config = mock_config
            _read_execution_section_from_file.cache_clear()
            result = get_memory_limit()
        assert result == 268435456

    def test_env_var_beats_config_file(self) -> None:
        """Env var takes priority over config file value."""
        from pyiceberg.execution.engine import (
            _read_execution_section_from_file,
            get_memory_limit,
        )

        mock_config = {"execution": {"memory-limit": 268435456}}  # 256 MB
        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__MEMORY_LIMIT": "134217728"}):  # 128 MB
            with patch("pyiceberg.utils.config.Config") as MockConfig:
                MockConfig.return_value.config = mock_config
                _read_execution_section_from_file.cache_clear()
                result = get_memory_limit()
        assert result == 134217728  # env var wins

    def test_return_type_is_int(self) -> None:
        """get_memory_limit must always return an int."""
        from pyiceberg.execution.engine import get_memory_limit

        result = get_memory_limit()
        assert isinstance(result, int)


class TestMemoryLimitConsumedByBackends:
    """Verify backend helper functions use get_memory_limit() instead of bare DEFAULT_MEMORY_LIMIT."""

    def test_datafusion_parse_memory_limit_uses_getter(self) -> None:
        """DataFusion's _resolve_memory_limit should use get_memory_limit() for default."""
        from pyiceberg.execution.backends.datafusion_backend import (
            _resolve_memory_limit,
        )
        from pyiceberg.execution.engine import get_memory_limit

        # When limit is None, should return the same value as get_memory_limit()
        assert _resolve_memory_limit(None) == get_memory_limit()

    def test_datafusion_parse_memory_limit_explicit_overrides(self) -> None:
        """Explicit limit value overrides the configured default."""
        from pyiceberg.execution.backends.datafusion_backend import (
            _resolve_memory_limit,
        )

        assert _resolve_memory_limit(1024) == 1024

    def test_datafusion_respects_env_var_for_default(self) -> None:
        """When no explicit limit, DataFusion should respect the env var config."""
        from pyiceberg.execution.backends.datafusion_backend import (
            _resolve_memory_limit,
        )

        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__MEMORY_LIMIT": "1073741824"}):
            result = _resolve_memory_limit(None)
        assert result == 1073741824  # 1 GB from env


class TestGetExecutionConfigInt:
    """Verify get_execution_config_int reads from env var, cached YAML, and default."""

    def test_returns_default_when_no_config(self) -> None:
        """With no config or env var, returns the provided default."""
        from pyiceberg.execution.engine import get_execution_config_int

        result = get_execution_config_int("nonexistent-key", 42)
        assert result == 42

    def test_env_var_overrides_default(self) -> None:
        """Env var PYICEBERG_EXECUTION__<KEY> overrides the default."""
        from pyiceberg.execution.engine import get_execution_config_int

        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__COW_THRESHOLD": "12345"}):
            result = get_execution_config_int("cow-threshold", 67108864)
        assert result == 12345

    def test_env_var_dashes_become_underscores(self) -> None:
        """Key 'oom-warning-threshold' maps to env var PYICEBERG_EXECUTION__OOM_WARNING_THRESHOLD."""
        from pyiceberg.execution.engine import get_execution_config_int

        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__OOM_WARNING_THRESHOLD": "999"}):
            result = get_execution_config_int("oom-warning-threshold", 2147483648)
        assert result == 999

    def test_invalid_env_var_falls_through_to_default(self) -> None:
        """Non-integer env var value is ignored, falls through to default."""
        from pyiceberg.execution.engine import get_execution_config_int

        with patch.dict(os.environ, {"PYICEBERG_EXECUTION__COW_THRESHOLD": "not_a_number"}):
            result = get_execution_config_int("cow-threshold", 64)
        assert result == 64

    def test_yaml_config_used_when_no_env_var(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Config file value is used when no env var is set."""
        from pyiceberg.execution.engine import (
            clear_config_cache,
            get_execution_config_int,
        )

        config_file = tmp_path / ".pyiceberg.yaml"
        config_file.write_text("execution:\n  cow-threshold: 33554432\n")
        monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))
        clear_config_cache()

        result = get_execution_config_int("cow-threshold", 67108864)
        assert result == 33554432

    def test_env_var_overrides_yaml_config(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Env var takes priority over YAML config file value."""
        from pyiceberg.execution.engine import (
            clear_config_cache,
            get_execution_config_int,
        )

        config_file = tmp_path / ".pyiceberg.yaml"
        config_file.write_text("execution:\n  cow-threshold: 33554432\n")
        monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))
        monkeypatch.setenv("PYICEBERG_EXECUTION__COW_THRESHOLD", "11111")
        clear_config_cache()

        result = get_execution_config_int("cow-threshold", 67108864)
        assert result == 11111

    def test_cached_section_avoids_repeated_disk_reads(self) -> None:
        """Multiple calls to get_execution_config_int share the cached section read."""
        from pyiceberg.execution.engine import (
            _read_execution_section_from_file,
            get_execution_config_int,
        )

        # Call twice -- cache_info should show hits on second call
        get_execution_config_int("cow-threshold", 64)
        get_execution_config_int("planning-threshold", 100000)

        info = _read_execution_section_from_file.cache_info()
        # At least one hit (the second call reuses the first's cache entry)
        assert info.hits >= 1

    def test_clear_config_cache_invalidates_section_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """clear_config_cache() forces re-read of the YAML section."""
        from pyiceberg.execution.engine import (
            clear_config_cache,
            get_execution_config_int,
        )

        # Set up initial config
        config_file = tmp_path / ".pyiceberg.yaml"
        config_file.write_text("execution:\n  cow-threshold: 100\n")
        monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))
        clear_config_cache()

        assert get_execution_config_int("cow-threshold", 0) == 100

        # "Edit" the config file
        config_file.write_text("execution:\n  cow-threshold: 200\n")
        clear_config_cache()

        assert get_execution_config_int("cow-threshold", 0) == 200
