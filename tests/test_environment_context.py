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

from pyiceberg.environment_context import EnvironmentContext


def test_default_value() -> None:
    actual = EnvironmentContext.get()
    assert len(actual) == 2
    assert actual["engine-name"] == "pyiceberg"
    assert re.match(r"^\d+\.\d+\.\d+", actual["engine-version"])


def test_put_and_remove() -> None:
    EnvironmentContext.put("test-key", "test-value")
    assert EnvironmentContext.get()["test-key"] == "test-value"

    EnvironmentContext.remove("test-key")
    assert "test-key" not in EnvironmentContext.get()
