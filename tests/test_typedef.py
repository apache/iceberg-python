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

from pyiceberg.typedef import FrozenDict, KeyDefaultDict, Record
from pyiceberg.types import IntegerType, NestedField, StructType


def test_setitem_frozendict() -> None:
    d = FrozenDict(foo=1, bar=2)
    with pytest.raises(AttributeError):
        d["foo"] = 3


def test_update_frozendict() -> None:
    d = FrozenDict(foo=1, bar=2)
    with pytest.raises(AttributeError):
        d.update({"yes": 2})


def test_keydefaultdict() -> None:
    def one(_: int) -> int:
        return 1

    defaultdict = KeyDefaultDict(one)
    assert defaultdict[22] == 1


def test_record_named_args() -> None:
    r = Record(1, "a", True)

    assert r[0] == 1
    assert r[1] == "a"
    assert r[2] is True

    assert repr(r) == "Record[1, a, True]"


def test_record_bind_rejects_unknown_arguments() -> None:
    struct = StructType(NestedField(1, "known", IntegerType()))

    with pytest.raises(TypeError, match="Unexpected Record fields: unknown"):
        Record._bind(struct, known=1, unknown=2)


def test_record_bind_rejects_non_schema_property() -> None:
    class RecordWithProperty(Record):
        @property
        def computed(self) -> int:
            return 1

    with pytest.raises(TypeError, match="Unexpected RecordWithProperty fields: computed"):
        RecordWithProperty._bind(StructType(), computed=1)
