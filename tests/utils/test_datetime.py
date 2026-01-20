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
from datetime import datetime, time, timezone, tzinfo

import pytest
import pytz

from pyiceberg.utils.datetime import (
    datetime_to_millis,
    datetime_to_nanos,
    millis_to_datetime,
    nanos_to_hours,
    nanos_to_micros,
    time_str_to_nanos,
    time_to_nanos,
    timestamp_to_nanos,
    timestamptz_to_nanos,
)

timezones = [
    pytz.timezone("Etc/GMT"),
    pytz.timezone("Etc/GMT+0"),
    pytz.timezone("Etc/GMT+1"),
    pytz.timezone("Etc/GMT+10"),
    pytz.timezone("Etc/GMT+11"),
    pytz.timezone("Etc/GMT+12"),
    pytz.timezone("Etc/GMT+2"),
    pytz.timezone("Etc/GMT+3"),
    pytz.timezone("Etc/GMT+4"),
    pytz.timezone("Etc/GMT+5"),
    pytz.timezone("Etc/GMT+6"),
    pytz.timezone("Etc/GMT+7"),
    pytz.timezone("Etc/GMT+8"),
    pytz.timezone("Etc/GMT+9"),
    pytz.timezone("Etc/GMT-0"),
    pytz.timezone("Etc/GMT-1"),
    pytz.timezone("Etc/GMT-10"),
    pytz.timezone("Etc/GMT-11"),
    pytz.timezone("Etc/GMT-12"),
    pytz.timezone("Etc/GMT-13"),
    pytz.timezone("Etc/GMT-14"),
    pytz.timezone("Etc/GMT-2"),
    pytz.timezone("Etc/GMT-3"),
    pytz.timezone("Etc/GMT-4"),
    pytz.timezone("Etc/GMT-5"),
    pytz.timezone("Etc/GMT-6"),
    pytz.timezone("Etc/GMT-7"),
    pytz.timezone("Etc/GMT-8"),
    pytz.timezone("Etc/GMT-9"),
]


def test_datetime_to_millis() -> None:
    dt = datetime(2023, 7, 10, 10, 10, 10, 123456)
    expected = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000)
    datetime_millis = datetime_to_millis(dt)
    assert datetime_millis == expected


@pytest.mark.parametrize("tz", timezones)
def test_datetime_tz_to_millis(tz: tzinfo) -> None:
    dt = datetime(2023, 7, 10, 10, 10, 10, 123456, tzinfo=tz)
    expected = int(dt.timestamp() * 1_000)
    datetime_millis = datetime_to_millis(dt)
    assert datetime_millis == expected


def test_millis_to_datetime() -> None:
    assert millis_to_datetime(1690971805918) == datetime(2023, 8, 2, 10, 23, 25, 918000)


@pytest.mark.parametrize("time_str, nanos", [("00:00:00+00:00", 0), ("20:21:44.375612-05:00", 73304375612000)])
def test_time_str_to_nanos(time_str: str, nanos: int) -> None:
    assert nanos == time_str_to_nanos(time_str)


@pytest.mark.parametrize(
    "time_, nanos", [(time(0, 0, 0), 0), (time(20, 21, 44, 375612, tzinfo=pytz.timezone("Etc/GMT-5")), 73304375612000)]
)
def test_time_to_nanos(time_: time, nanos: int) -> None:
    assert nanos == time_to_nanos(time_)


@pytest.mark.parametrize(
    "datetime_, nanos",
    [
        (datetime(1970, 1, 1, 0, 0, 0), 0),
        (datetime(2025, 2, 23, 20, 21, 44, 375612, tzinfo=pytz.timezone("Etc/GMT-5")), 1740324104375612000),
    ],
)
def test_datetime_to_nanos(datetime_: datetime, nanos: int) -> None:
    assert nanos == datetime_to_nanos(datetime_)


@pytest.mark.parametrize(
    "timestamp, nanos",
    [
        ("1970-01-01T00:00:00", 0),
        ("2025-02-23T20:21:44.375612", 1740342104375612000),
        ("2025-02-23T20:21:44.375612001", 1740342104375612001),
    ],
)
def test_timestamp_to_nanos(timestamp: str, nanos: int) -> None:
    assert nanos == timestamp_to_nanos(timestamp)


@pytest.mark.parametrize(
    "timestamp, nanos",
    [
        ("1970-01-01T00:00:00+00:00", 0),
        ("2025-02-23T16:21:44.375612-04:00", 1740342104375612000),
        ("2025-02-23T16:21:44.375612001-04:00", 1740342104375612001),
    ],
)
def test_timestamptz_to_nanos(timestamp: str, nanos: int) -> None:
    assert nanos == timestamptz_to_nanos(timestamp)


@pytest.mark.parametrize("nanos, micros", [(1510871468000001001, 1510871468000001), (-1510871468000001001, -1510871468000002)])
def test_nanos_to_micros(nanos: int, micros: int) -> None:
    assert micros == nanos_to_micros(nanos)


@pytest.mark.parametrize(
    "nanos, hours",
    [
        (1510871468000001001, 419686),
        (-1510871468000001001, -419687),
    ],
)
def test_nanos_to_hours(nanos: int, hours: int) -> None:
    assert hours == nanos_to_hours(nanos)
