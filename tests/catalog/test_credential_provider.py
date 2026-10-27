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

from __future__ import annotations

import pickle
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from pyiceberg.catalog.rest import LoadCredentialsResponse
from pyiceberg.catalog.rest.credential_provider import CredentialsProvider, resolve_storage_credentials
from pyiceberg.catalog.rest.scan_planning import StorageCredential
from pyiceberg.exceptions import ServiceUnavailableError
from pyiceberg.typedef import Properties


class _Clock:
    now = 1_700_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr("pyiceberg.catalog.rest.credential_provider.time", clock)
    return clock


class _Source:
    def __init__(self, *responses: LoadCredentialsResponse | Exception) -> None:
        self.responses = list(responses)
        self.calls = 0

    def __call__(self) -> LoadCredentialsResponse:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _credential(clock: _Clock, scheme: str = "s3", token: str = "original", lifetime: int = 60) -> StorageCredential:
    expires_at = str(int((clock.now + lifetime) * 1000))
    config = (
        {
            "s3.access-key-id": "access",
            "s3.secret-access-key": "secret",
            "s3.session-token": token,
            "s3.session-token-expires-at-ms": expires_at,
        }
        if scheme.startswith("s3")
        else {"gcs.oauth2.token": token, "gcs.oauth2.token-expires-at": expires_at}
    )
    return StorageCredential(prefix=f"{scheme}://warehouse/", config=config)


def _response(*credentials: StorageCredential) -> LoadCredentialsResponse:
    return LoadCredentialsResponse(storage_credentials=list(credentials))


def test_short_lived_credentials_refresh_once_per_interval(clock: _Clock) -> None:
    for scheme in ("s3", "s3a", "s3n", "gs", "gcs"):
        initial = _credential(clock, scheme)
        clock.now += 30
        rotated = _credential(clock, scheme, token="rotated")
        clock.now -= 30
        source = _Source(_response(rotated))
        provider = CredentialsProvider([initial], source)
        location = f"{scheme}://warehouse/data/file"

        assert provider.properties_for(location) == initial.config
        clock.now += 29
        assert provider.properties_for(location) == initial.config
        assert source.calls == 0
        clock.now += 1
        assert provider.properties_for(location) == rotated.config
        for _ in range(10):
            assert provider.properties_for(location) == rotated.config
        assert source.calls == 1


def test_long_lived_credentials_refresh_five_minutes_before_expiry(clock: _Clock) -> None:
    initial = _credential(clock, lifetime=3600)
    rotated = _credential(clock, token="rotated", lifetime=7200)
    source = _Source(_response(rotated))
    provider = CredentialsProvider([initial], source)

    clock.now += 3299
    assert provider.properties_for("s3://warehouse/file") == initial.config
    clock.now += 1
    assert provider.properties_for("s3://warehouse/file") == rotated.config
    assert source.calls == 1


def test_expired_credentials_refresh_before_use(clock: _Clock) -> None:
    for scheme in ("s3", "gs"):
        initial = _credential(clock, scheme, lifetime=-1)
        rotated = _credential(clock, scheme, token="rotated")
        source = _Source(_response(rotated))
        provider = CredentialsProvider([initial], source)
        assert provider.properties_for(f"{scheme}://warehouse/file") == rotated.config
        assert source.calls == 1


def test_static_and_unknown_scheme_credentials_do_not_refresh() -> None:
    credentials = [
        StorageCredential(prefix="s3://warehouse/", config={"s3.access-key-id": "static"}),
        StorageCredential(prefix="custom://warehouse/", config={"token": "static"}),
    ]
    source = _Source()
    provider = CredentialsProvider(credentials, source)
    for credential in credentials:
        assert provider.properties_for(credential.prefix + "file") == credential.config
    assert source.calls == 0


def test_longest_prefix_credentials_are_selected_and_copied(clock: _Clock) -> None:
    broad = _credential(clock)
    narrow = StorageCredential(prefix="s3://warehouse/table/", config={"s3.access-key-id": "narrow"})
    provider = CredentialsProvider([broad, narrow], _Source())
    selected = provider.properties_for("s3://warehouse/table/file")
    assert selected == narrow.config
    selected["s3.access-key-id"] = "changed"
    assert provider.properties_for("s3://warehouse/table/file") == {"s3.access-key-id": "narrow"}
    assert provider.properties_for("s3://other/file") == {}
    assert resolve_storage_credentials([broad, narrow], "s3://warehouse/table/file") == narrow.config
    assert resolve_storage_credentials([], "s3://warehouse/file") == {}
    assert resolve_storage_credentials([broad], None) == {}


def test_refresh_reselects_from_complete_replacement_list(clock: _Clock) -> None:
    initial = _credential(clock)
    broad = _credential(clock, token="broad", lifetime=3600)
    narrow = StorageCredential(prefix="s3://warehouse/table/", config={**broad.config, "s3.session-token": "narrow"})
    source = _Source(_response(broad, narrow))
    provider = CredentialsProvider([initial], source)
    clock.now += 30
    assert provider.properties_for("s3://warehouse/table/file") == narrow.config
    assert provider.properties_for("s3://warehouse/other/file") == broad.config
    assert source.calls == 1


def test_each_prefix_has_its_own_refresh_deadline(clock: _Clock) -> None:
    initial = _credential(clock)
    other = StorageCredential(prefix="gs://other/", config=_credential(clock, "gs", lifetime=3600).config)
    rotated = _credential(clock, token="rotated", lifetime=3600)
    source = _Source(_response(rotated, other))
    provider = CredentialsProvider([initial, other], source)
    clock.now += 30
    assert provider.properties_for("gs://other/file") == other.config
    assert source.calls == 0
    assert provider.properties_for("s3://warehouse/file") == rotated.config
    assert source.calls == 1


def test_failed_refresh_preserves_credentials_for_a_later_retry(clock: _Clock) -> None:
    initial = _credential(clock)
    rotated = _credential(clock, token="rotated", lifetime=3600)
    source = _Source(ServiceUnavailableError("catalog unavailable"), _response(rotated))
    provider = CredentialsProvider([initial], source)
    clock.now += 30
    with pytest.raises(ServiceUnavailableError, match="catalog unavailable"):
        provider.properties_for("s3://warehouse/file")
    assert provider.properties_for("s3://warehouse/file") == rotated.config
    assert source.calls == 2


def test_invalid_refresh_is_not_published(clock: _Clock) -> None:
    initial = _credential(clock)
    valid = _credential(clock, token="valid", lifetime=3600)
    invalid_responses = [
        _response(),
        _response(StorageCredential(prefix="s3://other/", config=valid.config)),
        _response(_credential(clock, lifetime=-1)),
        _response(StorageCredential(prefix=initial.prefix, config={"s3.access-key-id": "incomplete"})),
        _response(StorageCredential(prefix=initial.prefix, config={"s3.session-token-expires-at-ms": "opaque-secret"})),
    ]
    for invalid in invalid_responses:
        source = _Source(invalid, _response(valid))
        provider = CredentialsProvider([initial], source)
        clock.now += 30
        with pytest.raises(ValueError) as error:
            provider.properties_for("s3://warehouse/file")
        assert "opaque-secret" not in str(error.value)
        assert provider.properties_for("s3://warehouse/file") == valid.config
        assert source.calls == 2
        clock.now -= 30


def test_refreshable_credentials_require_complete_cloud_properties(clock: _Clock) -> None:
    for scheme, required_key in (("s3", "s3.secret-access-key"), ("gs", "gcs.oauth2.token")):
        credential = _credential(clock, scheme)
        credential.config.pop(required_key)
        with pytest.raises(ValueError, match=required_key):
            CredentialsProvider([credential], _Source())


def test_gcs_expiration_alias_is_normalized_and_canonical_property_wins(clock: _Clock) -> None:
    config: Properties = {"gcs.oauth2.token": "vended", "gcs.oauth2.token_expires_at": "1700000060000"}
    credential = StorageCredential(prefix="gs://warehouse/", config=config)
    provider = CredentialsProvider([credential], _Source())
    expected = {"gcs.oauth2.token": "vended", "gcs.oauth2.token-expires-at": "1700000060000"}
    assert provider.properties_for("gs://warehouse/file") == expected
    assert resolve_storage_credentials([credential], "gs://warehouse/file") == config
    assert credential.config == config
    config["gcs.oauth2.token-expires-at"] = "1700003600000"
    provider = CredentialsProvider([StorageCredential(prefix=credential.prefix, config=config)], _Source())
    assert provider.properties_for("gs://warehouse/file")["gcs.oauth2.token-expires-at"] == "1700003600000"


def test_provider_retains_refresh_deadline_after_pickle(clock: _Clock) -> None:
    initial = _credential(clock)
    rotated = _credential(clock, token="rotated", lifetime=3600)
    provider = CredentialsProvider([initial], _Source(_response(rotated)))
    clock.now += 29
    restored = pickle.loads(pickle.dumps(provider))
    assert restored.properties_for("s3://warehouse/file") == initial.config
    clock.now += 1
    assert restored.properties_for("s3://warehouse/file") == rotated.config


def test_concurrent_requests_share_one_credential_refresh(clock: _Clock) -> None:
    initial = _credential(clock)
    rotated = _credential(clock, token="rotated", lifetime=3600)
    entered = threading.Event()
    release = threading.Event()
    barrier = threading.Barrier(9)
    requests = []

    def fetch() -> LoadCredentialsResponse:
        requests.append("refresh")
        entered.set()
        assert release.wait(timeout=5)
        return _response(rotated)

    provider = CredentialsProvider([initial], fetch)
    clock.now += 30

    def read() -> Properties:
        barrier.wait(timeout=5)
        return provider.properties_for("s3://warehouse/file")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(read) for _ in range(8)]
        try:
            barrier.wait(timeout=5)
            assert entered.wait(timeout=5)
        finally:
            release.set()
        assert [future.result(timeout=5) for future in futures] == [rotated.config] * 8
    assert requests == ["refresh"]
