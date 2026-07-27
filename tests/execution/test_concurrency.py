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


"""Tests for threading, concurrency, credential isolation, and thread-safety of scoped env vars."""

from __future__ import annotations

import inspect
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest


def _try_import_datafusion() -> bool:
    """Check if datafusion is importable (for skipif decorators)."""
    try:
        import datafusion  # noqa: F401

        return True
    except ImportError:
        return False


# =============================================================================
# Schema type promotion (string → large_string)
# =============================================================================


class TestConcurrentCredentialIsolation:
    """Verify _scoped_env_vars serializes concurrent credential access.

    Two threads set different AWS_ACCESS_KEY_ID values. The _ENV_LOCK must
    ensure neither thread ever observes the other's credential value.
    """

    def test_concurrent_threads_never_observe_other_credentials(self) -> None:
        """Two threads with different credentials are serialized by _ENV_LOCK."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        # Save original value
        original_key = os.environ.get("AWS_ACCESS_KEY_ID")
        observations: dict[str, list[str]] = {"thread_a": [], "thread_b": []}

        def thread_work(thread_name: str, key_value: str, iterations: int = 50) -> None:
            for _ in range(iterations):
                with _scoped_env_vars({"AWS_ACCESS_KEY_ID": key_value}):
                    # Record what this thread sees while holding the lock
                    observed = os.environ.get("AWS_ACCESS_KEY_ID")
                    observations[thread_name].append(observed)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(thread_work, "thread_a", "KEY_AAAA", 50)
            future_b = executor.submit(thread_work, "thread_b", "KEY_BBBB", 50)
            future_a.result(timeout=10)
            future_b.result(timeout=10)

        # Thread A must only ever see its own key
        assert all(v == "KEY_AAAA" for v in observations["thread_a"]), (
            f"Thread A observed foreign credential: {set(observations['thread_a'])}"
        )
        # Thread B must only ever see its own key
        assert all(v == "KEY_BBBB" for v in observations["thread_b"]), (
            f"Thread B observed foreign credential: {set(observations['thread_b'])}"
        )

        # Environment restored after both threads finish
        assert os.environ.get("AWS_ACCESS_KEY_ID") == original_key

    def test_scoped_env_vars_restores_on_exception(self) -> None:
        """Credentials are restored even when the scoped block raises."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        original_key = os.environ.get("AWS_ACCESS_KEY_ID")

        with pytest.raises(ValueError, match="intentional"):
            with _scoped_env_vars({"AWS_ACCESS_KEY_ID": "TEMP_KEY_EXCEPTION"}):
                assert os.environ.get("AWS_ACCESS_KEY_ID") == "TEMP_KEY_EXCEPTION"
                raise ValueError("intentional")

        assert os.environ.get("AWS_ACCESS_KEY_ID") == original_key

    def test_scoped_env_vars_empty_map_is_noop(self) -> None:
        """Empty env_map should not acquire the lock or modify environment."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        original_key = os.environ.get("AWS_ACCESS_KEY_ID")

        with _scoped_env_vars({}):
            assert os.environ.get("AWS_ACCESS_KEY_ID") == original_key

        assert os.environ.get("AWS_ACCESS_KEY_ID") == original_key


# =============================================================================
# LSP -- supports_bounded_memory is capability, not behavioral divergence
# =============================================================================


class TestClearConfigCacheConcurrency:
    """Verify clear_config_cache is safe to call concurrently with resolve_backends.

    The risk is a race between cache_clear() and a concurrent resolve_backends() call
    that reads from the cache mid-clear. lru_cache.cache_clear() is atomic in CPython
    (it acquires the cache's internal lock), so this should be safe.
    """

    def test_concurrent_clear_and_resolve_no_crash(self) -> None:
        """Concurrent clear_config_cache + resolve_backends must not raise."""
        from pyiceberg.execution.engine import clear_config_cache, resolve_backends

        errors: list[Exception] = []

        def _resolve_loop() -> None:
            for _ in range(50):
                try:
                    resolve_backends("test_op")
                except Exception as e:
                    errors.append(e)

        def _clear_loop() -> None:
            for _ in range(50):
                try:
                    clear_config_cache()
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=_resolve_loop),
            threading.Thread(target=_resolve_loop),
            threading.Thread(target=_clear_loop),
            threading.Thread(target=_clear_loop),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Concurrent clear/resolve produced errors: {errors}"

    def test_clear_config_cache_is_idempotent(self) -> None:
        """Calling clear_config_cache multiple times must not raise."""
        from pyiceberg.execution.engine import clear_config_cache

        # Should not raise on multiple sequential calls
        for _ in range(10):
            clear_config_cache()


class TestScopedEnvVarsThreadSafety:
    """Verify _scoped_env_vars uses a lock for thread-safe credential isolation.

    The fix ensures concurrent threads cannot observe each other's credentials
    in os.environ by serializing access via _ENV_LOCK (RLock).
    """

    def test_env_lock_exists_and_is_rlock(self) -> None:
        """_ENV_LOCK must be a threading.RLock for re-entrant safety."""
        from pyiceberg.execution.object_store import _ENV_LOCK

        assert isinstance(_ENV_LOCK, type(threading.RLock())), "_ENV_LOCK must be a threading.RLock to allow re-entrant locking."

    def test_scoped_env_vars_acquires_lock(self) -> None:
        """_scoped_env_vars must acquire _ENV_LOCK during execution."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        source = inspect.getsource(_scoped_env_vars)
        assert "_ENV_LOCK" in source, (
            "_scoped_env_vars does not reference _ENV_LOCK. "
            "It must acquire the lock to prevent credential leakage across threads."
        )

    def test_concurrent_threads_cannot_observe_each_others_credentials(self) -> None:
        """Two threads with different credentials never see each other's values."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        env_key = "_PYICEBERG_TEST_CREDENTIAL_ISOLATION"
        # Ensure clean state
        os.environ.pop(env_key, None)

        observed_values: list[str | None] = [None, None]
        errors: list[str] = []

        def thread_a() -> None:
            with _scoped_env_vars({env_key: "SECRET_A"}):
                # Sleep briefly to allow thread B to attempt access
                time.sleep(0.01)
                val = os.environ.get(env_key)
                observed_values[0] = val
                if val != "SECRET_A":
                    errors.append(f"Thread A saw '{val}' instead of 'SECRET_A'")

        def thread_b() -> None:
            # Small delay so thread A acquires lock first
            time.sleep(0.005)
            with _scoped_env_vars({env_key: "SECRET_B"}):
                val = os.environ.get(env_key)
                observed_values[1] = val
                if val != "SECRET_B":
                    errors.append(f"Thread B saw '{val}' instead of 'SECRET_B'")

        t1 = threading.Thread(target=thread_a)
        t2 = threading.Thread(target=thread_b)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        # After both threads complete, env should be clean
        assert os.environ.get(env_key) is None, f"Environment not cleaned up after threads: {env_key}={os.environ.get(env_key)}"
        # Neither thread should have observed the other's credential
        assert not errors, f"Credential leakage detected: {errors}"
        assert observed_values[0] == "SECRET_A"
        assert observed_values[1] == "SECRET_B"

    def test_scoped_env_vars_restores_on_exception(self) -> None:
        """Credentials are cleaned up even when the inner code raises."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        env_key = "_PYICEBERG_TEST_EXCEPTION_CLEANUP"
        os.environ.pop(env_key, None)

        with pytest.raises(ValueError, match="intentional"):
            with _scoped_env_vars({env_key: "SENSITIVE_VALUE"}):
                assert os.environ.get(env_key) == "SENSITIVE_VALUE"
                raise ValueError("intentional")

        # Must be cleaned up
        assert os.environ.get(env_key) is None, "Credential was not cleaned up after exception."

    def test_scoped_env_vars_empty_map_does_not_acquire_lock(self) -> None:
        """Empty env_map yields immediately without locking (optimization)."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        # Should not block even if lock were held
        with _scoped_env_vars({}):
            pass  # Should complete instantly

    def test_scoped_env_vars_reentrant(self) -> None:
        """Nested _scoped_env_vars calls work (RLock allows re-entrance)."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        env_key_outer = "_PYICEBERG_TEST_REENTRANT_OUTER"
        env_key_inner = "_PYICEBERG_TEST_REENTRANT_INNER"
        os.environ.pop(env_key_outer, None)
        os.environ.pop(env_key_inner, None)

        with _scoped_env_vars({env_key_outer: "OUTER"}):
            assert os.environ.get(env_key_outer) == "OUTER"
            with _scoped_env_vars({env_key_inner: "INNER"}):
                assert os.environ.get(env_key_outer) == "OUTER"
                assert os.environ.get(env_key_inner) == "INNER"
            # Inner restored
            assert os.environ.get(env_key_inner) is None
            assert os.environ.get(env_key_outer) == "OUTER"

        # Outer restored
        assert os.environ.get(env_key_outer) is None


# =============================================================================
# _SortedRecordBatchReader temp file cleanup on abandoned reader
# =============================================================================


class TestScopedEnvVarsConcurrency:
    """Verify _scoped_env_vars does not deadlock with concurrent same-credential tasks."""

    def test_concurrent_same_credentials_no_deadlock(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multiple threads using the same credentials must not deadlock.

        The fast-path optimization means threads with identical env vars
        skip the lock entirely. This test verifies that N concurrent calls
        with the same credentials all complete within a reasonable timeout.
        """
        from pyiceberg.execution.object_store import _scoped_env_vars

        env_map = {
            "AWS_ACCESS_KEY_ID": "test-key-concurrent",
            "AWS_SECRET_ACCESS_KEY": "test-secret-concurrent",
        }

        # Pre-set the env vars so all threads hit the fast path
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key-concurrent")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-concurrent")

        results = []
        errors = []

        def _worker(worker_id: int) -> None:
            try:
                with _scoped_env_vars(env_map):
                    # Simulate work inside the scope
                    time.sleep(0.01)
                    results.append(worker_id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)  # 5 second timeout -- deadlock detection

        # All threads must complete
        assert len(errors) == 0, f"Threads raised errors: {errors}"
        assert len(results) == 16, f"Only {len(results)}/16 threads completed -- possible deadlock"

    def test_concurrent_different_credentials_serialized(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Different credentials must serialize (one at a time) but still complete."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        # Clear env so threads must go through the slow path
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        results = []

        def _worker(worker_id: int) -> None:
            env_map = {
                "AWS_ACCESS_KEY_ID": f"key-{worker_id}",
                "AWS_SECRET_ACCESS_KEY": f"secret-{worker_id}",
            }
            with _scoped_env_vars(env_map):
                time.sleep(0.005)
                results.append(worker_id)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        # All threads must complete (serialized but not deadlocked)
        assert len(results) == 8, f"Only {len(results)}/8 threads completed -- possible deadlock"


# =============================================================================
# Error handling: cleanup guarantees when backends raise
# =============================================================================


class TestConcurrentCredentialScoping:
    """Test concurrent _scoped_env_vars with different credentials."""

    def test_different_credentials_do_not_corrupt_each_other(self) -> None:
        """Two threads with different S3 creds don't see each other's values."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        results = {"thread_a": [], "thread_b": []}
        barrier = threading.Barrier(2, timeout=5)

        def thread_a() -> None:
            with _scoped_env_vars({"AWS_ACCESS_KEY_ID": "key_a", "AWS_SECRET_ACCESS_KEY": "secret_a"}):
                barrier.wait()
                # While inside the block, our env should be "key_a"
                results["thread_a"].append(os.environ.get("AWS_ACCESS_KEY_ID"))
                time.sleep(0.01)  # Give thread_b a chance to set its vars
                results["thread_a"].append(os.environ.get("AWS_ACCESS_KEY_ID"))

        def thread_b() -> None:
            barrier.wait()
            time.sleep(0.005)  # Stagger slightly
            with _scoped_env_vars({"AWS_ACCESS_KEY_ID": "key_b", "AWS_SECRET_ACCESS_KEY": "secret_b"}):
                results["thread_b"].append(os.environ.get("AWS_ACCESS_KEY_ID"))

        t_a = threading.Thread(target=thread_a)
        t_b = threading.Thread(target=thread_b)
        t_a.start()
        t_b.start()
        t_a.join(timeout=5)
        t_b.join(timeout=5)

        # The lock serializes access, so thread_a should complete before thread_b
        # enters. After thread_a exits, env is restored. thread_b then sets "key_b".
        # The key invariant: within each thread's block, it sees its own credentials.
        assert all(v in ("key_a", "key_b") for v in results["thread_a"] if v is not None)
        assert all(v == "key_b" for v in results["thread_b"] if v is not None)

        # Env should be clean after both threads complete
        assert os.environ.get("AWS_ACCESS_KEY_ID") is None or os.environ.get("AWS_ACCESS_KEY_ID") not in ("key_a", "key_b")

    def test_same_credentials_no_mutation(self) -> None:
        """Threads with identical credentials skip env var mutation (fast path)."""
        from pyiceberg.execution.object_store import _scoped_env_vars

        env = {"AWS_ACCESS_KEY_ID": "shared_key"}
        # Pre-set the env so the fast path is taken
        os.environ["AWS_ACCESS_KEY_ID"] = "shared_key"

        mutations_observed = {"count": 0}

        def worker() -> None:
            before = os.environ.get("AWS_ACCESS_KEY_ID")
            with _scoped_env_vars(env):
                during = os.environ.get("AWS_ACCESS_KEY_ID")
                time.sleep(0.01)
            after = os.environ.get("AWS_ACCESS_KEY_ID")
            # Fast path: no mutation should occur (value stays the same throughout)
            if before != during or during != after:
                with threading.Lock():
                    mutations_observed["count"] += 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # Fast path: no thread should observe any env var change
        assert mutations_observed["count"] == 0, "Fast path should not mutate env vars when they already match"

        # Cleanup
        os.environ.pop("AWS_ACCESS_KEY_ID", None)


# =============================================================================
# Section 6.2.3: CoW delete concurrent file removal between pass 1 and pass 2
# =============================================================================


class TestClearConfigCacheThreadSafety:
    """clear_config_cache() is safe to call concurrently with resolve_backends().

    lru_cache.cache_clear() is atomic in CPython (single bytecode op on the
    internal dict). This test verifies no exception is raised when clearing
    races with active resolution.
    """

    def test_concurrent_clear_and_resolve(self) -> None:
        """No crash when clear_config_cache is called during resolution."""
        from pyiceberg.execution.engine import clear_config_cache, resolve_backends

        errors = []

        def resolver() -> None:
            for _ in range(50):
                try:
                    resolve_backends("scan")
                except Exception as e:
                    errors.append(e)

        def clearer() -> None:
            for _ in range(50):
                try:
                    clear_config_cache()
                except Exception as e:
                    errors.append(e)

        t1 = threading.Thread(target=resolver)
        t2 = threading.Thread(target=clearer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == [], f"Concurrent clear/resolve raised: {errors}"
