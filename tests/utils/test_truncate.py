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
from pyiceberg.utils.truncate import truncate_upper_bound_binary_string, truncate_upper_bound_text_string


def test_upper_bound_string_truncation() -> None:
    assert truncate_upper_bound_text_string("aaaa", 2) == "ab"
    assert truncate_upper_bound_text_string("".join([chr(0x10FFFF), chr(0x10FFFF), chr(0x0)]), 2) is None


def test_upper_bound_string_truncation_skips_surrogates() -> None:
    # U+D7FF is the last scalar value before the surrogate range, so incrementing it
    # must skip to U+E000 rather than produce an unencodable lone surrogate.
    value = "a" + chr(0xD7FF) + "tail"

    result = truncate_upper_bound_text_string(value, 2)

    assert result == "a" + chr(0xE000)
    assert result >= value
    result.encode("utf-8")


def test_upper_bound_string_truncation_skips_surrogates_in_earlier_position() -> None:
    # The last character is at the maximum code point, so the increment falls back to the
    # previous character, which is also on the surrogate boundary.
    value = chr(0xD7FF) + chr(0x10FFFF) + "tail"

    result = truncate_upper_bound_text_string(value, 2)

    assert result == chr(0xE000) + chr(0x10FFFF)
    assert result >= value
    result.encode("utf-8")


def test_upper_bound_binary_truncation() -> None:
    assert truncate_upper_bound_binary_string(b"\x01\x02\x03", 2) == b"\x01\x03"
    assert truncate_upper_bound_binary_string(b"\xff\xff\x00", 2) is None
