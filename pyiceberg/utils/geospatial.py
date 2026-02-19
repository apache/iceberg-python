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

import math
import struct
from dataclasses import dataclass

_WKB_POINT = 1
_WKB_LINESTRING = 2
_WKB_POLYGON = 3
_WKB_MULTIPOINT = 4
_WKB_MULTILINESTRING = 5
_WKB_MULTIPOLYGON = 6
_WKB_GEOMETRYCOLLECTION = 7

_EWKB_Z_FLAG = 0x80000000
_EWKB_M_FLAG = 0x40000000
_EWKB_SRID_FLAG = 0x20000000


@dataclass(frozen=True)
class GeospatialBound:
    x: float
    y: float
    z: float | None = None
    m: float | None = None

    @property
    def has_z(self) -> bool:
        return self.z is not None

    @property
    def has_m(self) -> bool:
        return self.m is not None


@dataclass(frozen=True)
class GeometryEnvelope:
    x_min: float
    y_min: float
    z_min: float | None
    m_min: float | None
    x_max: float
    y_max: float
    z_max: float | None
    m_max: float | None

    def to_min_bound(self) -> GeospatialBound:
        return GeospatialBound(x=self.x_min, y=self.y_min, z=self.z_min, m=self.m_min)

    def to_max_bound(self) -> GeospatialBound:
        return GeospatialBound(x=self.x_max, y=self.y_max, z=self.z_max, m=self.m_max)


def serialize_geospatial_bound(bound: GeospatialBound) -> bytes:
    if bound.z is None and bound.m is None:
        return struct.pack("<dd", bound.x, bound.y)
    if bound.z is not None and bound.m is None:
        return struct.pack("<ddd", bound.x, bound.y, bound.z)
    if bound.z is None and bound.m is not None:
        return struct.pack("<dddd", bound.x, bound.y, math.nan, bound.m)
    return struct.pack("<dddd", bound.x, bound.y, bound.z, bound.m)


def deserialize_geospatial_bound(raw: bytes) -> GeospatialBound:
    if len(raw) == 16:
        x, y = struct.unpack("<dd", raw)
        return GeospatialBound(x=x, y=y)
    if len(raw) == 24:
        x, y, z = struct.unpack("<ddd", raw)
        return GeospatialBound(x=x, y=y, z=z)
    if len(raw) == 32:
        x, y, z, m = struct.unpack("<dddd", raw)
        return GeospatialBound(x=x, y=y, z=None if math.isnan(z) else z, m=m)
    raise ValueError(f"Invalid geospatial bound size: {len(raw)}")


def extract_envelope_from_wkb(wkb: bytes, is_geography: bool) -> GeometryEnvelope | None:
    reader = _WKBReader(wkb)
    accumulator = _EnvelopeAccumulator(is_geography=is_geography)
    _parse_geometry(reader, accumulator)
    if reader.remaining() != 0:
        raise ValueError(f"Trailing bytes found after parsing WKB: {reader.remaining()}")
    return accumulator.finish()


def merge_envelopes(left: GeometryEnvelope, right: GeometryEnvelope, is_geography: bool) -> GeometryEnvelope:
    if is_geography:
        x_min, x_max = _merge_longitude_intervals(left.x_min, left.x_max, right.x_min, right.x_max)
    else:
        x_min, x_max = min(left.x_min, right.x_min), max(left.x_max, right.x_max)

    return GeometryEnvelope(
        x_min=x_min,
        y_min=min(left.y_min, right.y_min),
        z_min=_merge_optional_min(left.z_min, right.z_min),
        m_min=_merge_optional_min(left.m_min, right.m_min),
        x_max=x_max,
        y_max=max(left.y_max, right.y_max),
        z_max=_merge_optional_max(left.z_max, right.z_max),
        m_max=_merge_optional_max(left.m_max, right.m_max),
    )


def _merge_optional_min(left: float | None, right: float | None) -> float | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _merge_optional_max(left: float | None, right: float | None) -> float | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


@dataclass
class _EnvelopeAccumulator:
    is_geography: bool
    x_min: float | None = None
    y_min: float | None = None
    z_min: float | None = None
    m_min: float | None = None
    x_max: float | None = None
    y_max: float | None = None
    z_max: float | None = None
    m_max: float | None = None
    longitudes: list[float] | None = None

    def __post_init__(self) -> None:
        if self.is_geography:
            self.longitudes = []

    def add_point(self, x: float, y: float, z: float | None, m: float | None) -> None:
        if math.isnan(x) or math.isnan(y):
            return

        if self.is_geography:
            if self.longitudes is None:
                self.longitudes = []
            self.longitudes.append(_normalize_longitude(x))
        else:
            self.x_min = x if self.x_min is None else min(self.x_min, x)
            self.x_max = x if self.x_max is None else max(self.x_max, x)

        self.y_min = y if self.y_min is None else min(self.y_min, y)
        self.y_max = y if self.y_max is None else max(self.y_max, y)

        if z is not None and not math.isnan(z):
            self.z_min = z if self.z_min is None else min(self.z_min, z)
            self.z_max = z if self.z_max is None else max(self.z_max, z)

        if m is not None and not math.isnan(m):
            self.m_min = m if self.m_min is None else min(self.m_min, m)
            self.m_max = m if self.m_max is None else max(self.m_max, m)

    def finish(self) -> GeometryEnvelope | None:
        if self.y_min is None or self.y_max is None:
            return None

        if self.is_geography:
            if not self.longitudes:
                return None
            x_min, x_max = _minimal_longitude_interval(self.longitudes)
        else:
            if self.x_min is None or self.x_max is None:
                return None
            x_min, x_max = self.x_min, self.x_max

        return GeometryEnvelope(
            x_min=x_min,
            y_min=self.y_min,
            z_min=self.z_min,
            m_min=self.m_min,
            x_max=x_max,
            y_max=self.y_max,
            z_max=self.z_max,
            m_max=self.m_max,
        )


class _WKBReader:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def remaining(self) -> int:
        return len(self._payload) - self._offset

    def read_byte(self) -> int:
        self._ensure_size(1)
        value = self._payload[self._offset]
        self._offset += 1
        return value

    def read_uint32(self, little_endian: bool) -> int:
        return self._read_fmt("<I" if little_endian else ">I")

    def read_double(self, little_endian: bool) -> float:
        return self._read_fmt("<d" if little_endian else ">d")

    def _read_fmt(self, fmt: str) -> float | int:
        size = struct.calcsize(fmt)
        self._ensure_size(size)
        value = struct.unpack_from(fmt, self._payload, self._offset)[0]
        self._offset += size
        return value

    def _ensure_size(self, expected: int) -> None:
        if self._offset + expected > len(self._payload):
            raise ValueError("Unexpected end of WKB payload")


def _parse_geometry(reader: _WKBReader, accumulator: _EnvelopeAccumulator) -> None:
    little_endian = _parse_byte_order(reader.read_byte())
    raw_type = reader.read_uint32(little_endian)
    geometry_type, has_z, has_m = _parse_geometry_type(raw_type)

    if raw_type & _EWKB_SRID_FLAG:
        reader.read_uint32(little_endian)

    if geometry_type == _WKB_POINT:
        _parse_point(reader, accumulator, little_endian, has_z, has_m)
    elif geometry_type == _WKB_LINESTRING:
        _parse_points(reader, accumulator, little_endian, has_z, has_m)
    elif geometry_type == _WKB_POLYGON:
        _parse_polygon(reader, accumulator, little_endian, has_z, has_m)
    elif geometry_type in (_WKB_MULTIPOINT, _WKB_MULTILINESTRING, _WKB_MULTIPOLYGON, _WKB_GEOMETRYCOLLECTION):
        _parse_collection(reader, accumulator, little_endian)
    else:
        raise ValueError(f"Unsupported WKB geometry type: {geometry_type}")


def _parse_byte_order(order: int) -> bool:
    if order == 1:
        return True
    if order == 0:
        return False
    raise ValueError(f"Unsupported WKB byte order marker: {order}")


def _parse_geometry_type(raw_type: int) -> tuple[int, bool, bool]:
    has_z = bool(raw_type & _EWKB_Z_FLAG)
    has_m = bool(raw_type & _EWKB_M_FLAG)
    type_code = raw_type & 0x1FFFFFFF

    if type_code >= 3000:
        has_z = True
        has_m = True
        type_code -= 3000
    elif type_code >= 2000:
        has_m = True
        type_code -= 2000
    elif type_code >= 1000:
        has_z = True
        type_code -= 1000

    return type_code, has_z, has_m


def _parse_collection(reader: _WKBReader, accumulator: _EnvelopeAccumulator, little_endian: bool) -> None:
    num_geometries = reader.read_uint32(little_endian)
    for _ in range(num_geometries):
        _parse_geometry(reader, accumulator)


def _parse_polygon(
    reader: _WKBReader,
    accumulator: _EnvelopeAccumulator,
    little_endian: bool,
    has_z: bool,
    has_m: bool,
) -> None:
    num_rings = reader.read_uint32(little_endian)
    for _ in range(num_rings):
        _parse_points(reader, accumulator, little_endian, has_z, has_m)


def _parse_points(
    reader: _WKBReader,
    accumulator: _EnvelopeAccumulator,
    little_endian: bool,
    has_z: bool,
    has_m: bool,
) -> None:
    count = reader.read_uint32(little_endian)
    for _ in range(count):
        x = reader.read_double(little_endian)
        y = reader.read_double(little_endian)
        if has_z and has_m:
            z = reader.read_double(little_endian)
            m = reader.read_double(little_endian)
        elif has_z:
            z = reader.read_double(little_endian)
            m = None
        elif has_m:
            z = None
            m = reader.read_double(little_endian)
        else:
            z = None
            m = None
        accumulator.add_point(x=x, y=y, z=z, m=m)


def _parse_point(
    reader: _WKBReader,
    accumulator: _EnvelopeAccumulator,
    little_endian: bool,
    has_z: bool,
    has_m: bool,
) -> None:
    x = reader.read_double(little_endian)
    y = reader.read_double(little_endian)

    if has_z and has_m:
        z = reader.read_double(little_endian)
        m = reader.read_double(little_endian)
    elif has_z:
        z = reader.read_double(little_endian)
        m = None
    elif has_m:
        z = None
        m = reader.read_double(little_endian)
    else:
        z = None
        m = None

    accumulator.add_point(x=x, y=y, z=z, m=m)


def _normalize_longitude(value: float) -> float:
    normalized = ((value + 180.0) % 360.0) - 180.0
    if math.isclose(normalized, -180.0) and value > 0:
        return 180.0
    return normalized


def _to_circle(value: float) -> float:
    if math.isclose(value, 180.0):
        return 360.0
    return value + 180.0


def _from_circle(value: float) -> float:
    if math.isclose(value, 360.0):
        return 180.0
    return value - 180.0


def _minimal_longitude_interval(longitudes: list[float]) -> tuple[float, float]:
    points = sorted({_to_circle(_normalize_longitude(v)) % 360.0 for v in longitudes})
    if len(points) == 1:
        lon = _from_circle(points[0])
        return lon, lon

    max_gap = -1.0
    max_gap_idx = 0
    for idx in range(len(points)):
        current = points[idx]
        nxt = points[(idx + 1) % len(points)] + (360.0 if idx == len(points) - 1 else 0.0)
        gap = nxt - current
        if gap > max_gap:
            max_gap = gap
            max_gap_idx = idx

    start = points[(max_gap_idx + 1) % len(points)]
    end = points[max_gap_idx]
    return _from_circle(start), _from_circle(end)


def _merge_longitude_intervals(
    left_min: float, left_max: float, right_min: float, right_max: float
) -> tuple[float, float]:
    segments = _interval_to_segments(left_min, left_max) + _interval_to_segments(right_min, right_max)
    merged = _merge_segments(segments)
    if not merged:
        raise ValueError("Cannot merge empty longitude intervals")

    largest_gap = -1.0
    gap_start = 0.0
    gap_end = 0.0
    for idx in range(len(merged)):
        current_end = merged[idx][1]
        next_start = merged[(idx + 1) % len(merged)][0] + (360.0 if idx == len(merged) - 1 else 0.0)
        gap = next_start - current_end
        if gap > largest_gap:
            largest_gap = gap
            gap_start = current_end
            gap_end = next_start

    if largest_gap <= 1e-12:
        return -180.0, 180.0

    start = gap_end % 360.0
    end = gap_start % 360.0
    return _from_circle(start), _from_circle(end)


def _interval_to_segments(x_min: float, x_max: float) -> list[tuple[float, float]]:
    start = _to_circle(_normalize_longitude(x_min))
    end = _to_circle(_normalize_longitude(x_max))

    if x_min <= x_max:
        return [(start, end)]
    return [(start, 360.0), (0.0, end)]


def _merge_segments(segments: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if not segments:
        return []

    ordered = sorted(segments, key=lambda pair: pair[0])
    merged: list[tuple[float, float]] = [ordered[0]]
    for start, end in ordered[1:]:
        previous_start, previous_end = merged[-1]
        if start <= previous_end:
            merged[-1] = (previous_start, max(previous_end, end))
        else:
            merged.append((start, end))
    return merged
