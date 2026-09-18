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


@pytest.mark.parametrize(
    "prefix, value",
    [
        pytest.param("fixed", "decimal[8]", id="wrong-prefix"),
        pytest.param("fixed", "fixed", id="missing-brackets"),
        pytest.param("truncate", "truncate[abc]", id="non-numeric"),
        pytest.param("truncate", "truncate[-1]", id="negative"),
    ],
)
def test_match_raises_with_expected_message(prefix: str, value: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        ParseNumberFromBrackets(prefix).match(value)
    assert str(exc_info.value) == f"Could not match {value}, expected format {prefix}[22]"
