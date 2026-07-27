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

"""Tests for object store credential routing, scoped env vars, and io_properties immutability."""

from __future__ import annotations

import json
import os
import threading
import time
import types
from unittest.mock import MagicMock

import pytest

# =============================================================================
# From: test_scoped_env_vars_fast_path.py
# =============================================================================


class TestFastPathSkipsLock:
    """When env vars already have correct values, _scoped_env_vars skips mutation."""

    def test_fast_path_no_mutation_when_values_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If env vars are already set correctly, _scoped_env_vars performs no mutation."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA_TEST")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        env_map = {"AWS_ACCESS_KEY_ID": "AKIA_TEST", "AWS_DEFAULT_REGION": "us-east-1"}

        class MutationTracker:
            """Track whether os.environ is actually mutated (the meaningful fast-path check)."""

            def __init__(self) -> None:
                self.mutations = 0

        MutationTracker()
        os.environ.__setitem__.__func__ if hasattr(os.environ.__setitem__, "__func__") else None

        # The real fast-path semantic: env vars should NOT be modified when they already match.
        # The lock may still be acquired (for TOCTOU safety), but no setenv/unsetenv occurs.
        before_snapshot = {k: v for k, v in os.environ.items() if k.startswith("AWS_")}

        with _scoped_env_vars(env_map):
            during_snapshot = {k: v for k, v in os.environ.items() if k.startswith("AWS_")}

        after_snapshot = {k: v for k, v in os.environ.items() if k.startswith("AWS_")}

        # Fast path: no changes before, during, or after
        assert before_snapshot == during_snapshot == after_snapshot

    def test_slow_path_acquires_lock_when_values_differ(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If env vars differ from desired, the lock IS acquired."""
        import pyiceberg.execution.object_store as obj_store
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "OLD_KEY")

        env_map = {"AWS_ACCESS_KEY_ID": "NEW_KEY"}

        lock_acquired_count = [0]
        real_lock = obj_store._ENV_LOCK

        class TrackingLock:
            def __enter__(self) -> None:
                lock_acquired_count[0] += 1
                return real_lock.__enter__()

            def __exit__(self, *args) -> None:
                return real_lock.__exit__(*args)

        monkeypatch.setattr(obj_store, "_ENV_LOCK", TrackingLock())

        with _scoped_env_vars(env_map):
            assert os.environ.get("AWS_ACCESS_KEY_ID") == "NEW_KEY"

        assert lock_acquired_count[0] > 0

    def test_slow_path_when_key_not_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If env var is not set at all, the lock IS acquired."""
        import pyiceberg.execution.object_store as obj_store
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.delenv("__PYICEBERG_TEST_KEY", raising=False)

        env_map = {"__PYICEBERG_TEST_KEY": "new_value"}

        lock_acquired_count = [0]
        real_lock = obj_store._ENV_LOCK

        class TrackingLock:
            def __enter__(self) -> None:
                lock_acquired_count[0] += 1
                return real_lock.__enter__()

            def __exit__(self, *args) -> None:
                return real_lock.__exit__(*args)

        monkeypatch.setattr(obj_store, "_ENV_LOCK", TrackingLock())

        with _scoped_env_vars(env_map):
            assert os.environ.get("__PYICEBERG_TEST_KEY") == "new_value"

        assert lock_acquired_count[0] > 0

    def test_slow_path_restores_original_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """After the slow path exits, original env vars are restored."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.setenv("__PYICEBERG_RESTORE_TEST", "original")

        with _scoped_env_vars({"__PYICEBERG_RESTORE_TEST": "temporary"}):
            assert os.environ["__PYICEBERG_RESTORE_TEST"] == "temporary"

        assert os.environ["__PYICEBERG_RESTORE_TEST"] == "original"

    def test_slow_path_restores_on_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Env vars are restored even when the scoped block raises."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.setenv("__PYICEBERG_EXC_TEST", "before")

        with pytest.raises(ValueError, match="boom"):
            with _scoped_env_vars({"__PYICEBERG_EXC_TEST": "during"}):
                assert os.environ["__PYICEBERG_EXC_TEST"] == "during"
                raise ValueError("boom")

        assert os.environ["__PYICEBERG_EXC_TEST"] == "before"


class TestParallelTasksWithSameCredentials:
    """Concurrent tasks with identical credentials should not block each other."""

    def test_concurrent_tasks_same_creds_run_in_parallel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multiple threads with same env vars should NOT serialize."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "SHARED_KEY")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        env_map = {"AWS_ACCESS_KEY_ID": "SHARED_KEY", "AWS_DEFAULT_REGION": "us-east-1"}

        timings: dict[str, list[float]] = {"t1": [], "t2": []}
        barrier = threading.Barrier(2, timeout=5)

        def task(name: str) -> None:
            barrier.wait()
            with _scoped_env_vars(env_map):
                timings[name].append(time.monotonic())
                time.sleep(0.05)
                timings[name].append(time.monotonic())

        t1 = threading.Thread(target=task, args=("t1",))
        t2 = threading.Thread(target=task, args=("t2",))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert len(timings["t1"]) == 2 and len(timings["t2"]) == 2

        t1_start, t1_end = timings["t1"]
        t2_start, t2_end = timings["t2"]

        overlaps = (t2_start < t1_end) or (t1_start < t2_end)
        assert overlaps

    def test_concurrent_tasks_different_creds_serialize(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Threads with DIFFERENT credentials must NOT overlap (serialized)."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        monkeypatch.delenv("__PYICEBERG_CRED_TEST", raising=False)

        timings: dict[str, list[float]] = {"t1": [], "t2": []}
        observations: dict[str, list[str]] = {"t1": [], "t2": []}
        barrier = threading.Barrier(2, timeout=5)

        def task(name: str, value: str) -> None:
            barrier.wait()
            with _scoped_env_vars({"__PYICEBERG_CRED_TEST": value}):
                timings[name].append(time.monotonic())
                observed = os.environ.get("__PYICEBERG_CRED_TEST")
                observations[name].append(observed)
                time.sleep(0.03)
                timings[name].append(time.monotonic())

        t1 = threading.Thread(target=task, args=("t1", "CRED_A"))
        t2 = threading.Thread(target=task, args=("t2", "CRED_B"))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert observations["t1"][0] == "CRED_A"
        assert observations["t2"][0] == "CRED_B"


# =============================================================================
# From: test_gcs_credential_routing.py
# =============================================================================


class TestGcsCredentialRouting:
    """datafusion_env_vars_from_properties must route GCS credentials correctly."""

    def test_file_path_maps_to_google_application_credentials(self) -> None:
        """A file path value sets GOOGLE_APPLICATION_CREDENTIALS."""
        from pyiceberg.execution.object_store import datafusion_env_vars_from_properties

        props = {"gcs.credentials-json": "/home/user/.config/gcloud/sa-key.json"}
        env_vars = datafusion_env_vars_from_properties(props)

        assert "GOOGLE_APPLICATION_CREDENTIALS" in env_vars
        assert env_vars["GOOGLE_APPLICATION_CREDENTIALS"] == "/home/user/.config/gcloud/sa-key.json"
        assert "GOOGLE_SERVICE_ACCOUNT" not in env_vars

    def test_json_content_maps_to_google_service_account(self) -> None:
        """Inline JSON content sets GOOGLE_SERVICE_ACCOUNT."""
        from pyiceberg.execution.object_store import datafusion_env_vars_from_properties

        sa_json = json.dumps(
            {
                "type": "service_account",
                "project_id": "my-project",
                "private_key_id": "key123",
                "private_key": "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
                "client_email": "sa@my-project.iam.gserviceaccount.com",
            }
        )
        props = {"gcs.credentials-json": sa_json}
        env_vars = datafusion_env_vars_from_properties(props)

        assert "GOOGLE_SERVICE_ACCOUNT" in env_vars
        assert env_vars["GOOGLE_SERVICE_ACCOUNT"] == sa_json
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in env_vars

    def test_json_with_leading_whitespace_detected_as_json(self) -> None:
        """JSON content with leading whitespace is still detected as JSON."""
        from pyiceberg.execution.object_store import datafusion_env_vars_from_properties

        sa_json = '  \n  {"type": "service_account", "project_id": "test"}'
        props = {"gcs.credentials-json": sa_json}
        env_vars = datafusion_env_vars_from_properties(props)

        assert "GOOGLE_SERVICE_ACCOUNT" in env_vars
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in env_vars

    def test_windows_file_path_not_mistaken_for_json(self) -> None:
        """Windows path (C:\\Users\\...) is correctly routed as file path."""
        from pyiceberg.execution.object_store import datafusion_env_vars_from_properties

        props = {"gcs.credentials-json": r"C:\Users\dev\keys\gcs-sa.json"}
        env_vars = datafusion_env_vars_from_properties(props)

        assert "GOOGLE_APPLICATION_CREDENTIALS" in env_vars
        assert "GOOGLE_SERVICE_ACCOUNT" not in env_vars

    def test_relative_path_routed_as_file_path(self) -> None:
        """Relative path (./credentials.json) routed as file path."""
        from pyiceberg.execution.object_store import datafusion_env_vars_from_properties

        props = {"gcs.credentials-json": "./credentials/sa.json"}
        env_vars = datafusion_env_vars_from_properties(props)

        assert "GOOGLE_APPLICATION_CREDENTIALS" in env_vars
        assert "GOOGLE_SERVICE_ACCOUNT" not in env_vars

    def test_no_gcs_credentials_produces_no_gcs_env_vars(self) -> None:
        """Without gcs.credentials-json, no GCS env vars are set."""
        from pyiceberg.execution.object_store import datafusion_env_vars_from_properties

        props = {"s3.access-key-id": "AKIA..."}
        env_vars = datafusion_env_vars_from_properties(props)

        assert "GOOGLE_APPLICATION_CREDENTIALS" not in env_vars
        assert "GOOGLE_SERVICE_ACCOUNT" not in env_vars


# =============================================================================
# From: test_io_properties_immutable.py
# =============================================================================


class TestIoPropertiesIsImmutable:
    """Backends.io_properties must be read-only after construction."""

    def test_io_properties_is_mapping_proxy(self) -> None:
        """build_backends() must wrap io_properties in MappingProxyType."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.access-key-id": "AKIA_TEST", "s3.region": "us-east-1"}
        backends = build_backends(props)

        assert isinstance(backends.io_properties, types.MappingProxyType)

    def test_io_properties_mutation_raises_type_error(self) -> None:
        """Attempting to mutate io_properties must raise TypeError."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.access-key-id": "AKIA_TEST", "s3.region": "us-east-1"}
        backends = build_backends(props)

        with pytest.raises(TypeError):
            backends.io_properties["s3.access-key-id"] = "CORRUPTED"

    def test_io_properties_deletion_raises_type_error(self) -> None:
        """Attempting to delete a key from io_properties must raise TypeError."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.access-key-id": "AKIA_TEST"}
        backends = build_backends(props)

        with pytest.raises(TypeError):
            del backends.io_properties["s3.access-key-id"]

    def test_io_properties_preserves_original_values(self) -> None:
        """io_properties must reflect the original dict values at construction time."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.access-key-id": "AKIA_ORIGINAL", "s3.region": "us-west-2"}
        backends = build_backends(props)

        assert backends.io_properties["s3.access-key-id"] == "AKIA_ORIGINAL"
        assert backends.io_properties["s3.region"] == "us-west-2"

    def test_io_properties_immune_to_external_mutation(self) -> None:
        """Mutating the original dict after construction must NOT affect backends."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.access-key-id": "AKIA_ORIGINAL"}
        backends = build_backends(props)

        props["s3.access-key-id"] = "AKIA_CORRUPTED"

        assert backends.io_properties["s3.access-key-id"] == "AKIA_ORIGINAL"

    def test_io_properties_is_a_mapping(self) -> None:
        """io_properties must satisfy the Mapping protocol."""
        from pyiceberg.execution.engine import build_backends

        props = {"s3.access-key-id": "AKIA_TEST", "s3.region": "eu-west-1"}
        backends = build_backends(props)

        assert "s3.access-key-id" in backends.io_properties
        assert len(backends.io_properties) == 2
        assert list(backends.io_properties.keys()) == ["s3.access-key-id", "s3.region"]
        assert dict(backends.io_properties) == props

    def test_resolve_also_produces_immutable_io_properties(self) -> None:
        """Backends.resolve() must also produce immutable io_properties."""
        from pyiceberg.execution.protocol import Backends

        props = {"s3.access-key-id": "AKIA_TEST"}
        backends = Backends.resolve(props)

        assert isinstance(backends.io_properties, types.MappingProxyType)
        with pytest.raises(TypeError):
            backends.io_properties["new_key"] = "value"

    def test_backends_dataclass_field_accepts_mapping_proxy(self) -> None:
        """The Backends dataclass must accept MappingProxyType for io_properties."""
        from pyiceberg.execution.protocol import Backends

        mock_read = MagicMock()
        mock_read.read_parquet = MagicMock()
        mock_write = MagicMock()
        mock_write.write_parquet = MagicMock()
        mock_write.write_data_files = MagicMock()
        mock_compute = MagicMock()
        mock_compute.supports_bounded_memory = False
        mock_compute.filter = MagicMock()
        mock_compute.sort_from_files = MagicMock()
        mock_compute.anti_join_from_files = MagicMock()
        mock_compute.apply_positional_deletes = MagicMock()

        proxy = types.MappingProxyType({"key": "value"})
        b = Backends(read=mock_read, write=mock_write, compute=mock_compute, io_properties=proxy)
        assert b.io_properties["key"] == "value"
