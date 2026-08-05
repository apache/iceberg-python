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

import threading
import time
from unittest.mock import MagicMock

from pyiceberg.catalog.rest import LoadCredentialsResponse
from pyiceberg.catalog.rest.credential_provider import CredentialsProvider, is_s3_credential_expired, resolve_storage_credentials
from pyiceberg.catalog.rest.scan_planning import StorageCredential
from pyiceberg.typedef import Properties

BASE_CREDENTIAL = StorageCredential(
    prefix="s3://warehouse/",
    config={
        "s3.access-key-id": "initial-key",
        "s3.secret-access-key": "initial-secret",
        "s3.session-token": "initial-token",
    },
)

LOCATION = "s3://warehouse/database/table/metadata/00001.json"


def _expiry_ms_in(seconds: float) -> str:
    return str(int((time.time() + seconds) * 1000))


def test_no_expiry_is_treated_as_static() -> None:
    assert is_s3_credential_expired({"s3.session-token": "token"}) is False


def test_far_expiry_is_not_expired() -> None:
    assert is_s3_credential_expired({"s3.session-token-expires-at-ms": _expiry_ms_in(3600)}) is False


def test_near_expiry_is_expired() -> None:
    assert is_s3_credential_expired({"s3.session-token-expires-at-ms": _expiry_ms_in(60)}) is True


def test_past_expiry_is_expired() -> None:
    assert is_s3_credential_expired({"s3.session-token-expires-at-ms": _expiry_ms_in(-60)}) is True


def test_threshold_boundary() -> None:
    # Just inside the threshold -> expired; well outside -> not expired
    assert is_s3_credential_expired({"s3.session-token-expires-at-ms": _expiry_ms_in(100)}, threshold_seconds=300) is True
    assert is_s3_credential_expired({"s3.session-token-expires-at-ms": _expiry_ms_in(600)}, threshold_seconds=300) is False


def _near_expiry_credential() -> StorageCredential:
    near_expiry_ms = str(int((time.time() + 60) * 1000))
    return StorageCredential(
        prefix="s3://warehouse/",
        config={**BASE_CREDENTIAL.config, "s3.session-token-expires-at-ms": near_expiry_ms},
    )


def _far_expiry_credential() -> StorageCredential:
    far_expiry_ms = str(int((time.time() + 3600) * 1000))
    return StorageCredential(
        prefix="s3://warehouse/",
        config={**BASE_CREDENTIAL.config, "s3.session-token-expires-at-ms": far_expiry_ms},
    )


def test_resolve_storage_credentials_longest_prefix_wins() -> None:
    credentials = [
        StorageCredential(prefix="s3://warehouse/", config={"s3.access-key-id": "short-prefix-key"}),
        StorageCredential(prefix="s3://warehouse/database/table", config={"s3.access-key-id": "long-prefix-key"}),
    ]
    assert resolve_storage_credentials(credentials, LOCATION) == {"s3.access-key-id": "long-prefix-key"}


def test_resolve_storage_credentials_empty() -> None:
    assert resolve_storage_credentials([], "s3://warehouse/foo") == {}
    assert resolve_storage_credentials([], None) == {}


def test_properties_for_multiple_prefixes_longest_match_wins() -> None:
    credentials = [
        StorageCredential(prefix="s3://warehouse/", config={"s3.access-key-id": "short-prefix-key"}),
        StorageCredential(prefix="s3://warehouse/database/table", config={"s3.access-key-id": "long-prefix-key"}),
    ]
    provider = CredentialsProvider(credentials, refresh_fn=MagicMock())
    assert provider.properties_for(LOCATION) == {"s3.access-key-id": "long-prefix-key"}


def test_properties_for_no_expiry_returns_static_creds_without_refresh() -> None:
    refresh_fn = MagicMock()
    provider = CredentialsProvider([BASE_CREDENTIAL], refresh_fn=refresh_fn)

    config = provider.properties_for(LOCATION)

    refresh_fn.assert_not_called()
    assert config == BASE_CREDENTIAL.config


def test_properties_for_far_expiry_does_not_refresh() -> None:
    refresh_fn = MagicMock()
    provider = CredentialsProvider([_far_expiry_credential()], refresh_fn=refresh_fn)

    provider.properties_for(LOCATION)

    refresh_fn.assert_not_called()


def test_properties_for_near_expiry_triggers_refresh_once() -> None:
    refreshed_credential = StorageCredential(
        prefix="s3://warehouse/",
        config={
            "s3.access-key-id": "refreshed-key",
            "s3.secret-access-key": "refreshed-secret",
            "s3.session-token": "refreshed-token",
        },
    )
    refresh_fn = MagicMock(return_value=LoadCredentialsResponse(storage_credentials=[refreshed_credential]))
    provider = CredentialsProvider([_near_expiry_credential()], refresh_fn=refresh_fn)

    config = provider.properties_for(LOCATION)

    refresh_fn.assert_called_once()
    assert config == refreshed_credential.config


def test_properties_for_empty_storage_credentials_returns_empty() -> None:
    provider = CredentialsProvider([], refresh_fn=MagicMock())
    assert provider.properties_for(LOCATION) == {}


def test_properties_for_no_match_returns_empty() -> None:
    provider = CredentialsProvider(
        [StorageCredential(prefix="s3://other-bucket/", config={"s3.access-key-id": "no-match"})], refresh_fn=MagicMock()
    )
    assert provider.properties_for(LOCATION) == {}


def test_properties_for_far_expiry_skips_lock_entirely() -> None:
    """The outer un-locked check should short-circuit before ever touching the lock."""
    refresh_fn = MagicMock()
    provider = CredentialsProvider([_far_expiry_credential()], refresh_fn=refresh_fn)
    mock_lock = MagicMock(spec=threading.Lock())
    provider._lock = mock_lock

    provider.properties_for(LOCATION)

    mock_lock.__enter__.assert_not_called()
    refresh_fn.assert_not_called()


def test_properties_for_concurrent_near_expiry_refreshes_exactly_once() -> None:
    """Double-checked locking: concurrent callers must trigger only a single refresh."""
    refreshed_credential = StorageCredential(
        prefix="s3://warehouse/",
        config={
            "s3.access-key-id": "refreshed-key",
            "s3.secret-access-key": "refreshed-secret",
            "s3.session-token": "refreshed-token",
        },
    )
    call_count = 0
    count_lock = threading.Lock()

    def slow_refresh_fn() -> LoadCredentialsResponse:
        nonlocal call_count
        with count_lock:
            call_count += 1
        # Widen the race window so other threads pile up waiting on the provider's lock
        # while this refresh is still in flight.
        time.sleep(0.2)
        return LoadCredentialsResponse(storage_credentials=[refreshed_credential])

    provider = CredentialsProvider([_near_expiry_credential()], refresh_fn=slow_refresh_fn)

    thread_count = 10
    barrier = threading.Barrier(thread_count)
    results: list[Properties] = []
    results_lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        config = provider.properties_for(LOCATION)
        with results_lock:
            results.append(config)

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert call_count == 1
    assert len(results) == thread_count
    assert all(config == refreshed_credential.config for config in results)


def test_properties_for_second_thread_reuses_refresh_done_by_first() -> None:
    """A thread that blocks on the lock must see the already-refreshed config, not trigger its own refresh."""
    refreshed_credential = StorageCredential(prefix="s3://warehouse/", config={"s3.access-key-id": "refreshed-key"})
    call_count = 0
    first_thread_holding_lock = threading.Event()
    release_first_thread = threading.Event()

    def refresh_fn() -> LoadCredentialsResponse:
        nonlocal call_count
        call_count += 1
        first_thread_holding_lock.set()
        release_first_thread.wait()
        return LoadCredentialsResponse(storage_credentials=[refreshed_credential])

    provider = CredentialsProvider([_near_expiry_credential()], refresh_fn=refresh_fn)

    first_result: list[Properties] = []
    second_result: list[Properties] = []

    def first_call() -> None:
        first_result.append(provider.properties_for(LOCATION))

    first_thread = threading.Thread(target=first_call)
    first_thread.start()
    first_thread_holding_lock.wait()

    # Second thread starts once the first is inside the lock refreshing; it should block on
    # the lock, then see the refreshed config once it acquires the lock and re-checks.
    second_thread = threading.Thread(target=lambda: second_result.append(provider.properties_for(LOCATION)))
    second_thread.start()

    release_first_thread.set()
    first_thread.join()
    second_thread.join()

    assert call_count == 1
    assert first_result[0] == refreshed_credential.config
    assert second_result[0] == refreshed_credential.config
