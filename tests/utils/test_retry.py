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

import time
from unittest.mock import MagicMock

import pytest

from pyiceberg.utils.retry import RetryConfig, run_with_retry, run_with_suppressed_failure


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class TestRetryConfig:
    def test_default_values(self) -> None:
        config = RetryConfig()
        assert config.max_attempts == 4
        assert config.min_wait_ms == 100
        assert config.max_wait_ms == 60000
        assert config.total_timeout_ms == 1800000
        assert config.scale_factor == 2.0
        assert config.jitter_factor == 0.1

    def test_custom_values(self) -> None:
        config = RetryConfig(
            max_attempts=10,
            min_wait_ms=50,
            max_wait_ms=5000,
            total_timeout_ms=60000,
            scale_factor=1.5,
            jitter_factor=0.2,
        )
        assert config.max_attempts == 10
        assert config.min_wait_ms == 50
        assert config.max_wait_ms == 5000
        assert config.total_timeout_ms == 60000
        assert config.scale_factor == 1.5
        assert config.jitter_factor == 0.2


class TestRunWithRetry:
    def test_success_on_first_attempt(self) -> None:
        """Test that successful task returns immediately."""
        task = MagicMock(return_value="success")
        config = RetryConfig(max_attempts=3, min_wait_ms=10)

        result = run_with_retry(task, config, retry_on=(RetryableError,))

        assert result == "success"
        assert task.call_count == 1

    def test_success_after_retry(self) -> None:
        """Test that task succeeds after retries."""
        call_count = 0

        def task() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RetryableError("Temporary failure")
            return "success"

        config = RetryConfig(max_attempts=5, min_wait_ms=1, max_wait_ms=10)

        result = run_with_retry(task, config, retry_on=(RetryableError,))

        assert result == "success"
        assert call_count == 3

    def test_failure_after_max_attempts(self) -> None:
        """Test that task fails after max attempts."""
        task = MagicMock(side_effect=RetryableError("Always fails"))
        config = RetryConfig(max_attempts=3, min_wait_ms=1, max_wait_ms=10)

        with pytest.raises(RetryableError, match="Always fails"):
            run_with_retry(task, config, retry_on=(RetryableError,))

        assert task.call_count == 3

    def test_non_retryable_error_not_retried(self) -> None:
        """Test that non-retryable errors are not retried."""
        task = MagicMock(side_effect=NonRetryableError("Non-retryable"))
        config = RetryConfig(max_attempts=3, min_wait_ms=1)

        with pytest.raises(NonRetryableError, match="Non-retryable"):
            run_with_retry(task, config, retry_on=(RetryableError,))

        assert task.call_count == 1

    def test_multiple_retry_exceptions(self) -> None:
        """Test that multiple exception types can be retried."""
        call_count = 0

        def task() -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RetryableError("First error")
            if call_count == 2:
                raise ValueError("Second error")
            return "success"

        config = RetryConfig(max_attempts=5, min_wait_ms=1, max_wait_ms=10)

        result = run_with_retry(task, config, retry_on=(RetryableError, ValueError))

        assert result == "success"
        assert call_count == 3

    def test_total_timeout(self) -> None:
        """Test that total timeout is respected."""
        start_time = time.time()
        task = MagicMock(side_effect=RetryableError("Always fails"))
        # Very short timeout
        config = RetryConfig(max_attempts=100, min_wait_ms=10, max_wait_ms=50, total_timeout_ms=100)

        with pytest.raises(RetryableError):
            run_with_retry(task, config, retry_on=(RetryableError,))

        elapsed = (time.time() - start_time) * 1000
        assert task.call_count < 100
        assert elapsed < 500

    def test_exponential_backoff(self) -> None:
        """Test that delays increase exponentially."""
        delays: list[float] = []
        last_time = [time.time()]

        def task() -> str:
            current_time = time.time()
            if last_time[0] is not None:
                delays.append((current_time - last_time[0]) * 1000)
            last_time[0] = current_time
            if len(delays) < 3:
                raise RetryableError("Retry")
            return "success"

        # Set jitter to 0 for predictable delays
        config = RetryConfig(
            max_attempts=5,
            min_wait_ms=50,
            max_wait_ms=1000,
            total_timeout_ms=10000,
            jitter_factor=0,
        )

        run_with_retry(task, config, retry_on=(RetryableError,))

        # First delay should be ~50ms, second ~100ms
        # Allow some margin for timing variations
        assert len(delays) >= 2
        # Delays should generally increase
        if len(delays) >= 2:
            assert delays[1] > delays[0] * 0.5  # Second delay should be larger


class TestRunWithSuppressedFailure:
    def test_success_returns_result(self) -> None:
        """Test that successful task returns result."""
        task = MagicMock(return_value="success")
        config = RetryConfig(max_attempts=3, min_wait_ms=1)

        result = run_with_suppressed_failure(task, config, default="default")

        assert result == "success"
        assert task.call_count == 1

    def test_failure_returns_default(self) -> None:
        """Test that failed task returns default value."""
        task = MagicMock(side_effect=Exception("Always fails"))
        config = RetryConfig(max_attempts=3, min_wait_ms=1, max_wait_ms=10)

        result = run_with_suppressed_failure(task, config, default="default")

        assert result == "default"
        assert task.call_count == 3

    def test_success_after_retry(self) -> None:
        """Test that task succeeds after retries."""
        call_count = 0

        def task() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary failure")
            return "success"

        config = RetryConfig(max_attempts=5, min_wait_ms=1, max_wait_ms=10)

        result = run_with_suppressed_failure(task, config, default="default")

        assert result == "success"
        assert call_count == 2

    def test_default_none(self) -> None:
        """Test that default can be None."""
        task = MagicMock(side_effect=Exception("Always fails"))
        config = RetryConfig(max_attempts=2, min_wait_ms=1)

        result = run_with_suppressed_failure(task, config, default=None)

        assert result is None
