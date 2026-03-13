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
import math
import struct

from pyiceberg.utils.geospatial import (
    GeospatialBound,
    deserialize_geospatial_bound,
    extract_envelope_from_wkb,
    merge_envelopes,
    serialize_geospatial_bound,
)


def test_geospatial_bound_serde_xy() -> None:
    raw = serialize_geospatial_bound(GeospatialBound(x=10.0, y=20.0))
    assert len(raw) == 16
    bound = deserialize_geospatial_bound(raw)
    assert bound.x == 10.0
    assert bound.y == 20.0
    assert bound.z is None
    assert bound.m is None


def test_geospatial_bound_serde_xyz() -> None:
    raw = serialize_geospatial_bound(GeospatialBound(x=10.0, y=20.0, z=30.0))
    assert len(raw) == 24
    bound = deserialize_geospatial_bound(raw)
    assert bound.x == 10.0
    assert bound.y == 20.0
    assert bound.z == 30.0
    assert bound.m is None


def test_geospatial_bound_serde_xym() -> None:
    raw = serialize_geospatial_bound(GeospatialBound(x=10.0, y=20.0, m=40.0))
    assert len(raw) == 32
    x, y, z, m = struct.unpack("<dddd", raw)
    assert x == 10.0
    assert y == 20.0
    assert math.isnan(z)
    assert m == 40.0

    bound = deserialize_geospatial_bound(raw)
    assert bound.x == 10.0
    assert bound.y == 20.0
    assert bound.z is None
    assert bound.m == 40.0


def test_geospatial_bound_serde_xyzm() -> None:
    raw = serialize_geospatial_bound(GeospatialBound(x=10.0, y=20.0, z=30.0, m=40.0))
    assert len(raw) == 32
    bound = deserialize_geospatial_bound(raw)
    assert bound.x == 10.0
    assert bound.y == 20.0
    assert bound.z == 30.0
    assert bound.m == 40.0


def test_extract_envelope_geometry() -> None:
    # LINESTRING(170 0, -170 1)
    wkb = struct.pack("<BIIdddd", 1, 2, 2, 170.0, 0.0, -170.0, 1.0)
    envelope = extract_envelope_from_wkb(wkb, is_geography=False)
    assert envelope is not None
    assert envelope.x_min == -170.0
    assert envelope.x_max == 170.0
    assert envelope.y_min == 0.0
    assert envelope.y_max == 1.0


def test_extract_envelope_geography_wraps_antimeridian() -> None:
    # LINESTRING(170 0, -170 1)
    wkb = struct.pack("<BIIdddd", 1, 2, 2, 170.0, 0.0, -170.0, 1.0)
    envelope = extract_envelope_from_wkb(wkb, is_geography=True)
    assert envelope is not None
    assert envelope.x_min > envelope.x_max
    assert envelope.x_min == 170.0
    assert envelope.x_max == -170.0
    assert envelope.y_min == 0.0
    assert envelope.y_max == 1.0


def test_extract_envelope_xyzm_linestring() -> None:
    # LINESTRING ZM (0 1 2 3, 4 5 6 7)
    wkb = struct.pack("<BII" + "dddd" * 2, 1, 3002, 2, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
    envelope = extract_envelope_from_wkb(wkb, is_geography=False)
    assert envelope is not None
    assert envelope.x_min == 0.0
    assert envelope.x_max == 4.0
    assert envelope.y_min == 1.0
    assert envelope.y_max == 5.0
    assert envelope.z_min == 2.0
    assert envelope.z_max == 6.0
    assert envelope.m_min == 3.0
    assert envelope.m_max == 7.0


def test_merge_geography_envelopes() -> None:
    left = extract_envelope_from_wkb(struct.pack("<BIIdddd", 1, 2, 2, 170.0, 0.0, -170.0, 1.0), is_geography=True)
    right = extract_envelope_from_wkb(struct.pack("<BIIdddd", 1, 2, 2, -160.0, 2.0, -120.0, 3.0), is_geography=True)
    assert left is not None
    assert right is not None

    merged = merge_envelopes(left, right, is_geography=True)
    assert merged.x_min > merged.x_max
    assert merged.x_min == 170.0
    assert merged.x_max == -120.0
    assert merged.y_min == 0.0
    assert merged.y_max == 3.0
