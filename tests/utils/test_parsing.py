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

import pytest

from pyiceberg.exceptions import ValidationError
from pyiceberg.utils.parsing import ParseNumberFromBrackets


def test_match_returns_the_bracketed_number() -> None:
    assert ParseNumberFromBrackets("fixed").match("fixed[22]") == 22
    assert ParseNumberFromBrackets("bucket").match("bucket[8]") == 8
    assert ParseNumberFromBrackets("truncate").match("truncate[16]") == 16


def test_match_reads_multi_digit_values() -> None:
    assert ParseNumberFromBrackets("fixed").match("fixed[1024]") == 1024


def test_match_ignores_text_around_the_prefix() -> None:
    # the implementation searches for the pattern, so surrounding text is tolerated
    assert ParseNumberFromBrackets("fixed").match("  fixed[5]  ") == 5
    assert ParseNumberFromBrackets("bucket").match("transform=bucket[4]") == 4


def test_match_returns_the_first_occurrence() -> None:
    assert ParseNumberFromBrackets("bucket").match("bucket[3] bucket[7]") == 3


def test_match_raises_for_a_different_prefix() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ParseNumberFromBrackets("fixed").match("decimal[8]")
    assert "expected format fixed[22]" in str(exc_info.value)


def test_match_raises_when_brackets_are_missing() -> None:
    with pytest.raises(ValidationError):
        ParseNumberFromBrackets("fixed").match("fixed")


def test_match_raises_for_a_non_numeric_argument() -> None:
    with pytest.raises(ValidationError):
        ParseNumberFromBrackets("truncate").match("truncate[abc]")


def test_match_raises_for_a_negative_number() -> None:
    # the pattern only accepts digits, so a leading minus sign does not match
    with pytest.raises(ValidationError):
        ParseNumberFromBrackets("truncate").match("truncate[-1]")
