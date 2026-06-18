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
from decimal import Decimal

import pytest

from pyiceberg.utils.decimal import bytes_required, decimal_required_bytes, decimal_to_bytes


def test_decimal_required_bytes() -> None:
    assert decimal_required_bytes(precision=1) == 1
    assert decimal_required_bytes(precision=2) == 1
    assert decimal_required_bytes(precision=3) == 2
    assert decimal_required_bytes(precision=4) == 2
    assert decimal_required_bytes(precision=5) == 3
    assert decimal_required_bytes(precision=7) == 4
    assert decimal_required_bytes(precision=8) == 4
    assert decimal_required_bytes(precision=10) == 5
    assert decimal_required_bytes(precision=32) == 14
    assert decimal_required_bytes(precision=38) == 16

    with pytest.raises(ValueError) as exc_info:
        decimal_required_bytes(precision=40)
    assert "(0, 40]" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        decimal_required_bytes(precision=-1)
    assert "(0, 40]" in str(exc_info.value)


def test_bytes_required() -> None:
    # Positive values and the negative values just past a byte boundary are unaffected.
    assert bytes_required(0) == 1
    assert bytes_required(127) == 1
    assert bytes_required(128) == 2
    assert bytes_required(-127) == 1
    assert bytes_required(-129) == 2
    # The most-negative value that fits in N bytes (-2**(8N-1)) must require exactly N bytes,
    # not N + 1. These are the cases the previous (value.bit_length() + 8) // 8 formula overcounted.
    assert bytes_required(-128) == 1
    assert bytes_required(-32768) == 2
    assert bytes_required(-8388608) == 3
    # The same applies when the unscaled value comes from a Decimal.
    assert bytes_required(Decimal("1.27")) == 1
    assert bytes_required(Decimal("-1.28")) == 1
    assert bytes_required(Decimal("-327.68")) == 2


def test_decimal_to_bytes() -> None:
    # Check the boundary between 2 and 3 bytes.
    # 2 bytes has a minimum of -32,768 and a maximum value of 32,767 (inclusive).
    assert decimal_to_bytes(Decimal("32767.")) == b"\x7f\xff"
    assert decimal_to_bytes(Decimal("32768.")) == b"\x00\x80\x00"
    # The most-negative value for a given width must serialize to the minimum number of bytes
    # (matching the Iceberg spec / Java BigInteger.toByteArray), not one byte longer.
    assert decimal_to_bytes(Decimal("-1.28")) == b"\x80"
    assert decimal_to_bytes(Decimal("-327.68")) == b"\x80\x00"
