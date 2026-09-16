#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


_SURROGATE_START = 0xD800
_SURROGATE_END = 0xDFFF
_MAX_CODE_POINT = 0x10FFFF


def _next_code_point(char: str) -> str | None:
    """Return the next Unicode scalar value after char, or None if there is none."""
    code_point = ord(char) + 1
    # Surrogates are not scalar values and cannot be encoded as UTF-8, so skip the range.
    if _SURROGATE_START <= code_point <= _SURROGATE_END:
        code_point = _SURROGATE_END + 1
    if code_point > _MAX_CODE_POINT:
        return None
    return chr(code_point)


def truncate_upper_bound_text_string(value: str, trunc_length: int | None) -> str | None:
    result = value[:trunc_length]
    if result != value:
        chars = [*result]

        for i in range(-1, -len(result) - 1, -1):
            if (next_char := _next_code_point(chars[i])) is not None:
                chars[i] = next_char
                return "".join(chars)
        return None  # didn't find a valid upper bound
    return result


def truncate_upper_bound_binary_string(value: bytes, trunc_length: int | None) -> bytes | None:
    result = value[:trunc_length]
    if result != value:
        _bytes = [*result]
        for i in range(-1, -len(result) - 1, -1):
            if _bytes[i] < 255:
                _bytes[i] += 1
                return b"".join([i.to_bytes(1, byteorder="little") for i in _bytes])
        return None

    return result
