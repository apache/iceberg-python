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

"""Retry utilities for Iceberg operations."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryConfig:
    """Configuration for retry behavior with exponential backoff."""

    def __init__(
        self,
        max_attempts: int = 4,
        min_wait_ms: int = 100,
        max_wait_ms: int = 60000,
        total_timeout_ms: int = 1800000,
        scale_factor: float = 2.0,
        jitter_factor: float = 0.1,
    ):
        """Initialize retry configuration.

        Args:
            max_attempts: Maximum number of retry attempts. Must be >= 1.
            min_wait_ms: Minimum wait time between retries in milliseconds. Must be >= 0.
            max_wait_ms: Maximum wait time between retries in milliseconds. Must be >= 0.
            total_timeout_ms: Total time allowed for all retries in milliseconds. Must be >= 0.
            scale_factor: Exponential backoff scale factor.
            jitter_factor: Random jitter factor (0.1 = 10% jitter).

        Note:
            Values <= 0 are clamped to minimum valid values to match Java behavior:
            - max_attempts: minimum 1 (at least one attempt)
            - min_wait_ms, max_wait_ms, total_timeout_ms: minimum 0
        """
        self.max_attempts = max(1, max_attempts)
        self.min_wait_ms = max(0, min_wait_ms)
        self.max_wait_ms = max(0, max_wait_ms)
        self.total_timeout_ms = max(0, total_timeout_ms)
        self.scale_factor = scale_factor
        self.jitter_factor = jitter_factor


def run_with_retry(
    task: Callable[[], T],
    config: RetryConfig,
    retry_on: tuple[type[Exception], ...],
) -> T:
    """Run a task with retry logic using exponential backoff.

    Args:
        task: The callable task to execute.
        config: Retry configuration parameters.
        retry_on: Tuple of exception types that should trigger a retry.

    Returns:
        The result of the task.

    Raises:
        The last exception if all retries fail.
    """
    start_time_ms = int(time.time() * 1000)
    attempt = 0

    while True:
        attempt += 1
        try:
            return task()
        except Exception as e:
            if not isinstance(e, retry_on):
                raise

            duration_ms = int(time.time() * 1000) - start_time_ms

            if attempt >= config.max_attempts:
                logger.info("Stopping retries after %d attempts", attempt)
                raise type(e)(f"{e} (stopped after {attempt} attempts, max_attempts={config.max_attempts})") from e

            if duration_ms > config.total_timeout_ms and attempt > 1:
                logger.info("Stopping retries after %d ms (timeout)", duration_ms)
                raise type(e)(f"{e} (stopped after {duration_ms}ms, total_timeout_ms={config.total_timeout_ms})") from e

            delay_ms = min(
                config.min_wait_ms * (config.scale_factor ** (attempt - 1)),
                config.max_wait_ms,
            )
            jitter = random.random() * delay_ms * config.jitter_factor
            sleep_time_ms = delay_ms + jitter

            logger.warning(
                "Retrying task after failure (attempt %d): sleepTimeMs=%.0f, error=%s",
                attempt,
                sleep_time_ms,
                str(e),
            )

            time.sleep(sleep_time_ms / 1000)


def run_with_suppressed_failure(
    task: Callable[[], T],
    config: RetryConfig,
    default: T | None = None,
) -> T | None:
    """Run a task with retry logic, suppressing final failure.

    This is useful for status checks where we want to keep trying but
    return a default value if all attempts fail.

    Args:
        task: The callable task to execute.
        config: Retry configuration parameters.
        default: Default value to return if all attempts fail.

    Returns:
        The result of the task, or the default value if all attempts fail.
    """
    start_time_ms = int(time.time() * 1000)
    attempt = 0

    while True:
        attempt += 1
        try:
            return task()
        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_time_ms

            if attempt >= config.max_attempts:
                logger.error(
                    "Cannot complete task after %d attempts, returning default",
                    attempt,
                )
                return default

            if duration_ms > config.total_timeout_ms and attempt > 1:
                logger.error(
                    "Cannot complete task after %d ms (timeout), returning default",
                    duration_ms,
                )
                return default

            delay_ms = min(
                config.min_wait_ms * (config.scale_factor ** (attempt - 1)),
                config.max_wait_ms,
            )
            jitter = random.random() * delay_ms * config.jitter_factor
            sleep_time_ms = delay_ms + jitter

            logger.warning(
                "Retrying task after failure (attempt %d): sleepTimeMs=%.0f, error=%s",
                attempt,
                sleep_time_ms,
                str(e),
            )

            time.sleep(sleep_time_ms / 1000)
