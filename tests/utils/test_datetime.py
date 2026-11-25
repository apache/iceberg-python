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
import re
from datetime import UTC, datetime, time, tzinfo

import pytest
import pytz

from pyiceberg.utils.datetime import (
    datetime_to_millis,
    datetime_to_nanos,
    days_to_date,
    micros_to_days,
    millis_to_datetime,
    nanos_to_hours,
    nanos_to_micros,
    time_str_to_nanos,
    time_to_nanos,
    timestamp_to_micros,
    timestamp_to_nanos,
    timestamptz_to_micros,
    timestamptz_to_nanos,
    to_human_day,
    to_human_hour,
    to_human_month,
    to_human_time,
    to_human_timestamp,
    to_human_timestamptz,
    to_human_year,
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
    expected = int(dt.replace(tzinfo=UTC).timestamp() * 1_000)
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
        ("2025-02-23T20:21:44.3756127", 1740342104375612700),
        ("2025-02-23T20:21:44.37561278", 1740342104375612780),
        ("2025-02-23T20:21:44.375612001", 1740342104375612001),
    ],
)
def test_timestamp_to_nanos(timestamp: str, nanos: int) -> None:
    assert nanos == timestamp_to_nanos(timestamp)


def test_timestamp_to_nanos_unexpected_zone_offset() -> None:
    with pytest.raises(ValueError, match="Zone offset provided, but not expected: 2025-02-23T16:21:44.375612001-04:00"):
        timestamp_to_nanos("2025-02-23T16:21:44.375612001-04:00")


def test_timestamp_to_nanos_invalid_timestamp() -> None:
    with pytest.raises(ValueError, match=re.escape("Invalid timestamp without zone: invalid (must be ISO-8601)")):
        timestamp_to_nanos("invalid")


@pytest.mark.parametrize(
    "timestamp, nanos",
    [
        ("1970-01-01T00:00:00+00:00", 0),
        ("2025-02-23T16:21:44.375612-04:00", 1740342104375612000),
        ("2025-02-23T16:21:44.3756127-04:00", 1740342104375612700),
        ("2025-02-23T16:21:44.37561278-04:00", 1740342104375612780),
        ("2025-02-23T16:21:44.375612001-04:00", 1740342104375612001),
    ],
)
def test_timestamptz_to_nanos(timestamp: str, nanos: int) -> None:
    assert nanos == timestamptz_to_nanos(timestamp)


def test_timestamptz_to_nanos_missing_zone_offset() -> None:
    with pytest.raises(ValueError, match="Missing zone offset: 2025-02-23T20:21:44.375612001"):
        timestamptz_to_nanos("2025-02-23T20:21:44.375612001")


def test_timestamptz_to_nanos_invalid_timestamp() -> None:
    with pytest.raises(ValueError, match=re.escape("Invalid timestamp with zone: invalid (must be ISO-8601)")):
        timestamptz_to_nanos("invalid")


@pytest.mark.parametrize(
    "timestamp, micros",
    [
        ("1970-01-01T00:00:00", 0),
        ("2025-02-23T20:21:44.375612", 1740342104375612),
        ("2025-02-23T20:21:44.3756127", 1740342104375612),
        ("2025-02-23T20:21:44.37561278", 1740342104375612),
        ("2025-02-23T20:21:44.375612001", 1740342104375612),
    ],
)
def test_timestamp_to_micros(timestamp: str, micros: int) -> None:
    assert micros == timestamp_to_micros(timestamp)


def test_timestamp_to_micros_unexpected_zone_offset() -> None:
    with pytest.raises(ValueError, match="Zone offset provided, but not expected: 2025-02-23T16:21:44.375612001-04:00"):
        timestamp_to_micros("2025-02-23T16:21:44.375612001-04:00")


def test_timestamp_to_micros_invalid_timestamp() -> None:
    with pytest.raises(ValueError, match=re.escape("Invalid timestamp without zone: invalid (must be ISO-8601)")):
        timestamp_to_micros("invalid")


@pytest.mark.parametrize(
    "timestamp, micros",
    [
        ("1970-01-01T00:00:00+00:00", 0),
        ("2025-02-23T16:21:44.375612-04:00", 1740342104375612),
        ("2025-02-23T16:21:44.3756127-04:00", 1740342104375612),
        ("2025-02-23T16:21:44.37561278-04:00", 1740342104375612),
        ("2025-02-23T16:21:44.375612001-04:00", 1740342104375612),
        ("2025-02-23T16:21:44-04:00:00", 1740342104000000),
    ],
)
def test_timestamptz_to_micros(timestamp: str, micros: int) -> None:
    assert micros == timestamptz_to_micros(timestamp)


def test_timestamptz_to_micros_missing_zone_offset() -> None:
    with pytest.raises(ValueError, match="Missing zone offset: 2025-02-23T20:21:44.375612001"):
        timestamptz_to_micros("2025-02-23T20:21:44.375612001")


def test_timestamptz_to_micros_invalid_timestamp() -> None:
    with pytest.raises(ValueError, match=re.escape("Invalid timestamp with zone: invalid (must be ISO-8601)")):
        timestamptz_to_micros("invalid")


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


def test_micros_to_days() -> None:
    for micros, days in [(0, 0), (86_400_000_000, 1), (-1, -1)]:
        assert micros_to_days(micros) == days


def test_days_to_date() -> None:
    for days, expected in [
        (0, "1970-01-01"),
        (31, "1970-02-01"),
        (-1, "1969-12-31"),
        # Proleptic Gregorian (per the Iceberg spec) has no gap at the 1582 Julian/Gregorian switch.
        (-141438, "1582-10-04"),
        (-141437, "1582-10-05"),
        (-141427, "1582-10-15"),
    ]:
        assert days_to_date(days).isoformat() == expected


def test_to_human_year() -> None:
    for ordinal, expected in [(0, "1970"), (1, "1971"), (-1, "1969"), (30, "2000")]:
        assert to_human_year(ordinal) == expected


def test_to_human_month() -> None:
    for ordinal, expected in [(0, "1970-01"), (11, "1970-12"), (12, "1971-01"), (13, "1971-02"), (-1, "1969-12")]:
        assert to_human_month(ordinal) == expected


def test_to_human_day() -> None:
    for ordinal, expected in [(0, "1970-01-01"), (31, "1970-02-01"), (-1, "1969-12-31")]:
        assert to_human_day(ordinal) == expected


def test_to_human_hour() -> None:
    for ordinal, expected in [(0, "1970-01-01-00"), (1, "1970-01-01-01"), (24, "1970-01-02-00"), (-1, "1969-12-31-23")]:
        assert to_human_hour(ordinal) == expected


def test_to_human_time() -> None:
    for micros, expected in [(0, "00:00:00"), (3_661_000_000, "01:01:01"), (73_304_375_612, "20:21:44.375612")]:
        assert to_human_time(micros) == expected


def test_to_human_timestamp() -> None:
    for micros, expected in [
        (0, "1970-01-01T00:00:00"),
        (-1, "1969-12-31T23:59:59.999999"),
        (1_740_342_104_375_612, "2025-02-23T20:21:44.375612"),
    ]:
        assert to_human_timestamp(micros) == expected


def test_to_human_timestamptz() -> None:
    for micros, expected in [
        (0, "1970-01-01T00:00:00+00:00"),
        (-1, "1969-12-31T23:59:59.999999+00:00"),
        (1_740_342_104_375_612, "2025-02-23T20:21:44.375612+00:00"),
    ]:
        assert to_human_timestamptz(micros) == expected
