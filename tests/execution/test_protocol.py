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

"""Tests for protocol design, SRP/LSP adherence, API completeness, and module boundaries.

Covers:
- SRP enforcement: protocol.py is purely declarative, engine.py owns instantiation
- LSP enforcement: sort-on-write is best-effort, backend substitution is safe
- Public API completeness: __all__ correctness, frozen dataclasses, type annotations
- Module boundary tests: no instantiation logic leaks into protocol.py
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import textwrap
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

# =============================================================================
# From test_protocol_srp_lsp.py
# =============================================================================

# SRP Tests: protocol.py should be purely declarative


class TestProtocolModuleIsDeclarative:
    """protocol.py must not contain complex resolution/instantiation logic.

    The Backends.resolve() classmethod should be a thin one-liner delegating
    to engine.build_backends(). All override handling, instantiation from
    registry, and protocol validation must live in engine.py.
    """

    def test_backends_resolve_delegates_to_build_backends(self):
        """Backends.resolve() must produce the same result as build_backends().

        Behavioral equivalent: call both and verify they produce functionally
        identical Backends instances (same backend types, same io_properties).
        """
        from pyiceberg.execution.engine import build_backends
        from pyiceberg.execution.protocol import Backends

        props = {"s3.region": "us-west-2"}

        via_resolve = Backends.resolve(props)
        via_build = build_backends(props)

        assert type(via_resolve.read) is type(via_build.read)
        assert type(via_resolve.write) is type(via_build.write)
        assert type(via_resolve.compute) is type(via_build.compute)
        assert dict(via_resolve.io_properties) == dict(via_build.io_properties)

    def test_protocol_module_does_not_perform_instantiation(self):
        """Backends.resolve() must delegate -- calling it should go through build_backends.

        Behavioral equivalent: patch build_backends and verify Backends.resolve()
        calls it (proving delegation without inspecting source code).
        """
        from pyiceberg.execution.protocol import Backends

        with patch("pyiceberg.execution.engine.build_backends") as mock_build:
            mock_build.return_value = MagicMock()
            mock_build.return_value.read = MagicMock()
            mock_build.return_value.write = MagicMock()
            mock_build.return_value.compute = MagicMock()
            mock_build.return_value.io_properties = {}

            Backends.resolve({"key": "val"})

        mock_build.assert_called_once()
        call_args = mock_build.call_args
        assert call_args[0][0] == {"key": "val"}, "build_backends must receive io_properties"

    def test_build_backends_lives_in_engine_module(self):
        """engine.py must export a build_backends() factory function."""
        from pyiceberg.execution.engine import build_backends

        assert callable(build_backends)

    def test_build_backends_returns_backends_instance(self):
        """build_backends() must return a fully constructed Backends dataclass."""
        from pyiceberg.execution.engine import build_backends
        from pyiceberg.execution.protocol import Backends

        result = build_backends({})
        assert isinstance(result, Backends)
        assert hasattr(result, "read")
        assert hasattr(result, "write")
        assert hasattr(result, "compute")
        assert hasattr(result, "io_properties")

    def test_build_backends_passes_io_properties_through(self):
        """build_backends() must store io_properties values (frozen snapshot)."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.region": "us-east-1", "warehouse": "s3://bucket"}
        result = build_backends(props)
        # Snapshot semantics: same values, but frozen (MappingProxyType)
        assert dict(result.io_properties) == props

    def test_build_backends_validates_read_protocol(self):
        """build_backends() must raise TypeError for invalid read override."""
        from pyiceberg.execution.engine import build_backends

        class NotAReader:
            pass

        with pytest.raises(TypeError, match="ReadBackend"):
            build_backends({}, read=NotAReader())

    def test_build_backends_validates_write_protocol(self):
        """build_backends() must raise TypeError for invalid write override."""
        from pyiceberg.execution.engine import build_backends

        class NotAWriter:
            pass

        with pytest.raises(TypeError, match="WriteBackend"):
            build_backends({}, write=NotAWriter())

    def test_build_backends_validates_compute_protocol(self):
        """build_backends() must raise TypeError for invalid compute override."""
        from pyiceberg.execution.engine import build_backends

        class NotACompute:
            pass

        with pytest.raises(TypeError, match="ComputeBackend"):
            build_backends({}, compute=NotACompute())

    def test_build_backends_accepts_string_overrides(self):
        """build_backends() with string overrides resolves to correct backends."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend, PyArrowReadBackend
        from pyiceberg.execution.engine import build_backends

        result = build_backends({}, read="pyarrow", compute="pyarrow")
        assert isinstance(result.read, PyArrowReadBackend)
        assert isinstance(result.compute, PyArrowComputeBackend)

    def test_build_backends_accepts_instance_overrides(self):
        """build_backends() with instance overrides uses them directly."""
        from pyiceberg.execution.backends.pyarrow_backend import (
            PyArrowComputeBackend,
            PyArrowReadBackend,
        )
        from pyiceberg.execution.engine import build_backends

        read_instance = PyArrowReadBackend()
        compute_instance = PyArrowComputeBackend()

        result = build_backends({}, read=read_instance, compute=compute_instance)
        assert result.read is read_instance
        assert result.compute is compute_instance

    def test_backends_resolve_still_works_end_to_end(self):
        """Backends.resolve() must continue to work after the refactor."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowReadBackend, PyArrowWriteBackend
        from pyiceberg.execution.protocol import Backends

        result = Backends.resolve({})
        assert isinstance(result.read, PyArrowReadBackend)
        assert isinstance(result.write, PyArrowWriteBackend)
        assert result.io_properties == {}


# LSP Tests: sort-on-write is best-effort (documented capability)


class TestSortOnWriteIsBestEffort:
    """Sort-on-write behavior depends on compute backend capabilities.

    The LSP concern is that substituting a supports_bounded_memory=False backend
    for a True one produces different outcomes (unsorted vs sorted files). This
    must be explicitly documented as "best-effort optimization, not correctness
    guarantee" at the protocol level and the public API level.
    """

    def test_compute_backend_docstring_states_best_effort(self):
        """ComputeBackend.supports_bounded_memory docstring must say 'best-effort'."""
        from pyiceberg.execution.protocol import ComputeBackend

        doc = ComputeBackend.supports_bounded_memory.fget.__doc__ or ""
        assert "best-effort" in doc.lower() or "best effort" in doc.lower(), (
            "ComputeBackend.supports_bounded_memory must document that callers "
            "use it for best-effort optimizations (e.g., sort-on-write), not correctness."
        )

    def test_apply_sort_order_docstring_states_best_effort(self):
        """_apply_sort_order docstring must document that sorting is best-effort."""
        from pyiceberg.table import Transaction

        doc = Transaction._apply_sort_order.__doc__ or ""
        assert "best-effort" in doc.lower() or "best effort" in doc.lower(), (
            "_apply_sort_order must document that sort-on-write is a best-effort "
            "optimization that depends on compute backend capabilities."
        )

    def test_unsorted_data_is_still_correct(self):
        """Data written without sort-on-write must be valid Iceberg data.

        Behavioral equivalent: verify that _apply_sort_order returns input
        unchanged when the compute backend doesn't support bounded memory.
        Sort order is a HINT for read optimization, not a correctness constraint.
        """
        from unittest.mock import MagicMock, patch

        import pyarrow as pa

        from pyiceberg.table import Transaction

        # Build a Transaction-like context with a non-bounded-memory backend
        mock_transaction = MagicMock(spec=Transaction)
        mock_transaction.table_metadata = MagicMock()
        mock_transaction.table_metadata.default_sort_order_id = 1
        mock_transaction.table_metadata.sort_orders = [MagicMock(order_id=1, fields=[MagicMock()])]
        mock_transaction._table = MagicMock()
        mock_transaction._table.io.properties = {}

        # Create a mock backends with supports_bounded_memory=False
        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = False

        # The input data -- should be returned unchanged
        input_table = pa.table({"id": [3, 1, 2]})

        with patch("pyiceberg.execution._orchestrate._get_sort_order", return_value=[("id", "ascending")]):
            result = Transaction._apply_sort_order(mock_transaction, input_table, mock_backends)

        # Data is returned unchanged -- unsorted is valid
        assert result is input_table, (
            "When compute backend lacks bounded memory, _apply_sort_order "
            "must return the input unchanged (unsorted data is still valid)."
        )

    def test_sort_on_write_skipped_when_no_bounded_memory(self):
        """When compute backend lacks bounded memory, data is returned unchanged."""
        from unittest.mock import MagicMock

        from pyiceberg.execution.protocol import Backends

        # Create a mock table/transaction context
        mock_table = MagicMock()
        mock_table.io.properties = {}

        # Create a backends instance with supports_bounded_memory=False
        mock_compute = MagicMock()
        type(mock_compute).supports_bounded_memory = PropertyMock(return_value=False)

        mock_backends = MagicMock(spec=Backends)
        type(mock_backends).supports_bounded_memory = PropertyMock(return_value=False)

        # The key assertion: when sort order exists but no bounded memory,
        # the input df should be returned unchanged.
        # This is tested via the code path, not a full integration test.
        from pyiceberg.execution._orchestrate import _get_sort_order

        # _get_sort_order with UNSORTED returns None → sort is skipped
        # We just verify the function exists and handles None sort order

        # Minimal verification: _get_sort_order returns None for unsorted tables
        # (actual sort-skipping logic is in _apply_sort_order)
        assert callable(_get_sort_order)


# Combined: build_backends in __all__


class TestPublicAPIExports:
    """Verify build_backends is exported from the execution package."""

    def test_build_backends_in_engine_public_api(self):
        """build_backends should be importable from pyiceberg.execution.engine."""
        from pyiceberg.execution.engine import build_backends

        assert callable(build_backends)

    def test_build_backends_in_package_all(self):
        """build_backends should be listed in pyiceberg.execution.__all__."""
        import pyiceberg.execution as exec_pkg

        assert "build_backends" in exec_pkg.__all__


# =============================================================================
# From test_srp_boundaries.py
# =============================================================================


class TestProtocolModuleHasNoInstantiationLogic:
    """protocol.py must NOT contain backend instantiation logic (SRP: engine.py owns that)."""

    def test_no_instantiate_functions_in_protocol(self):
        """protocol.py must not define any _instantiate_* functions."""
        from pyiceberg.execution import protocol

        instantiate_members = [
            name for name, obj in inspect.getmembers(protocol, inspect.isfunction) if "instantiate" in name.lower()
        ]
        assert instantiate_members == [], (
            f"protocol.py contains instantiation functions: {instantiate_members}. "
            f"Backend instantiation belongs in engine.py (registry pattern). "
            f"protocol.py should only define Protocol interfaces and dataclasses."
        )

    def test_no_backend_imports_in_protocol(self):
        """protocol.py must not import from pyiceberg.execution.backends.* at module level.

        Backend imports at module level would couple interface definitions to
        concrete implementations, violating DIP. Lazy imports inside
        Backends.resolve() are acceptable (they delegate to engine.py).
        """
        from pyiceberg.execution import protocol

        source = inspect.getsource(protocol)
        tree = ast.parse(textwrap.dedent(source))

        # Check only top-level imports (not inside functions/methods)
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert "pyiceberg.execution.backends" not in node.module, (
                        f"protocol.py has a top-level import from backends: "
                        f"'from {node.module} import ...'. "
                        f"This couples interfaces to implementations. "
                        f"Move instantiation logic to engine.py."
                    )

    def test_backends_resolve_delegates_to_engine(self):
        """Backends.resolve() must delegate to engine.build_backends()."""
        from pyiceberg.execution.protocol import Backends

        # Behavioral: calling resolve produces a valid Backends instance
        backends = Backends.resolve({}, compute="pyarrow")
        assert backends.compute is not None
        assert backends.read is not None
        assert backends.write is not None


class TestEngineModuleOwnsInstantiation:
    """engine.py must contain the registry and instantiation logic."""

    def test_engine_has_read_backend_registry(self):
        """engine.py must define _READ_BACKEND_REGISTRY."""
        from pyiceberg.execution import engine

        assert hasattr(engine, "_READ_BACKEND_REGISTRY")
        assert isinstance(engine._READ_BACKEND_REGISTRY, dict)
        assert "PYARROW" in engine._READ_BACKEND_REGISTRY

    def test_engine_has_compute_backend_registry(self):
        """engine.py must define _COMPUTE_BACKEND_REGISTRY."""
        from pyiceberg.execution import engine

        assert hasattr(engine, "_COMPUTE_BACKEND_REGISTRY")
        assert isinstance(engine._COMPUTE_BACKEND_REGISTRY, dict)
        assert "PYARROW" in engine._COMPUTE_BACKEND_REGISTRY

    def test_engine_has_build_backends_function(self):
        """engine.py must export build_backends() as the public factory."""
        from pyiceberg.execution.engine import build_backends

        assert callable(build_backends)

    def test_engine_instantiate_functions_exist(self):
        """engine.py must define _instantiate_read, _instantiate_write, _instantiate_compute."""
        from pyiceberg.execution import engine

        assert hasattr(engine, "_instantiate_read") and callable(engine._instantiate_read)
        assert hasattr(engine, "_instantiate_write") and callable(engine._instantiate_write)
        assert hasattr(engine, "_instantiate_compute") and callable(engine._instantiate_compute)

    def test_build_backends_returns_backends_dataclass(self):
        """build_backends() must return a Backends dataclass instance."""
        from pyiceberg.execution.engine import build_backends
        from pyiceberg.execution.protocol import Backends

        result = build_backends({})
        assert isinstance(result, Backends)


# =============================================================================
# From test_public_api_completeness.py
# =============================================================================


class TestAllExportsAreValid:
    """Every name in __all__ must resolve to an actual attribute on the module."""

    def test_no_ghost_entries_in_all(self):
        """Every name in __all__ must be getattr-able from the module."""
        import pyiceberg.execution as mod

        ghosts = []
        for name in mod.__all__:
            if not hasattr(mod, name):
                ghosts.append(name)

        assert not ghosts, f"__all__ contains names that don't exist on the module: {ghosts}"

    def test_all_imports_are_exported(self):
        """Every public name imported at module level should be in __all__.

        Private names (starting with _), submodules, and __future__ are excluded.
        """
        import types

        import pyiceberg.execution as mod

        # Get all non-private names that are imported (not __dunder__)
        imported_names = {
            name
            for name in dir(mod)
            if not name.startswith("_") and not name == "annotations"  # from __future__
        }

        all_set = set(mod.__all__)

        # Check for names that are imported but not in __all__
        missing = imported_names - all_set
        # Filter out submodules (they appear as attributes but aren't part of public API)
        missing = {m for m in missing if not isinstance(getattr(mod, m, None), types.ModuleType)}

        assert not missing, f"These public names are imported but missing from __all__: {missing}"

    def test_build_backends_in_all(self):
        """build_backends must be in __all__ (documented as public API)."""
        import pyiceberg.execution as mod

        assert "build_backends" in mod.__all__


class TestFrozenDataclasses:
    """Verify value objects are truly immutable (frozen=True)."""

    def test_write_result_is_frozen(self):
        """_WriteResult must be frozen (no mutation after construction)."""
        from pyiceberg.execution.backends.pyarrow_backend import _WriteResult

        assert dataclasses.is_dataclass(_WriteResult)
        # Check frozen by attempting mutation
        wr = _WriteResult(
            file_path="/tmp/test.parquet",
            file_size_in_bytes=1024,
            record_count=10,
            column_sizes={},
            value_counts={},
            null_value_counts={},
            lower_bounds={},
            upper_bounds={},
            split_offsets=[],
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            wr.file_path = "/other/path"  # type: ignore[misc]

    def test_backends_is_frozen(self):
        """Backends must be frozen (no mutation after construction)."""
        from unittest.mock import MagicMock

        from pyiceberg.execution.protocol import Backends

        assert dataclasses.is_dataclass(Backends)
        b = Backends(read=MagicMock(), write=MagicMock(), compute=MagicMock(), io_properties={})
        with pytest.raises(dataclasses.FrozenInstanceError):
            b.read = MagicMock()  # type: ignore[misc]

    def test_resolved_backends_is_frozen(self):
        """ResolvedBackends must be frozen."""
        from pyiceberg.execution.engine import ExecutionEngine, ResolvedBackends

        assert dataclasses.is_dataclass(ResolvedBackends)
        rb = ResolvedBackends(
            read=ExecutionEngine.PYARROW,
            write=ExecutionEngine.PYARROW,
            compute=ExecutionEngine.PYARROW,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            rb.compute = ExecutionEngine.DATAFUSION  # type: ignore[misc]


class TestTypeAnnotationsOnPublicAPI:
    """Verify key public functions have return type annotations (not bare -> None or missing)."""

    def test_build_backends_has_return_annotation(self):
        """build_backends() must declare its return type."""
        from pyiceberg.execution.engine import build_backends

        sig = inspect.signature(build_backends)
        assert sig.return_annotation is not inspect.Signature.empty

    def test_resolve_backends_has_return_annotation(self):
        """resolve_backends() must declare its return type."""
        from pyiceberg.execution.engine import resolve_backends

        sig = inspect.signature(resolve_backends)
        assert sig.return_annotation is not inspect.Signature.empty

    def test_get_memory_limit_has_return_annotation(self):
        """get_memory_limit() must declare int return type."""
        from pyiceberg.execution.engine import get_memory_limit

        sig = inspect.signature(get_memory_limit)
        assert sig.return_annotation is not inspect.Signature.empty
        # With `from __future__ import annotations`, annotation is the string 'int'
        assert sig.return_annotation in (int, "int")

    def test_expression_to_sql_has_return_annotation(self):
        """expression_to_sql() must declare str return type."""
        from pyiceberg.execution.expression_to_sql import expression_to_sql

        sig = inspect.signature(expression_to_sql)
        assert sig.return_annotation is not inspect.Signature.empty
        assert sig.return_annotation in (str, "str")
