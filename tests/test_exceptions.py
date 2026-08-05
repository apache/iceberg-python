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

from pyiceberg.exceptions import (
    NoSuchSnapshotRefError,
    NotAncestorError,
    SnapshotRefTypeError,
)


def test_no_such_snapshot_ref_error_is_value_error() -> None:
    with pytest.raises(ValueError):
        raise NoSuchSnapshotRefError("nope")


def test_snapshot_ref_type_error_is_value_error() -> None:
    with pytest.raises(ValueError):
        raise SnapshotRefTypeError("nope")


def test_not_ancestor_error_is_value_error() -> None:
    with pytest.raises(ValueError):
        raise NotAncestorError("nope")
