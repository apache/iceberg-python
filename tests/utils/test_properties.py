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

from pyiceberg.utils.properties import (
    get_first_property_value,
    property_as_bool,
    property_as_float,
    property_as_int,
)


def test_property_as_int() -> None:
    properties = {
        "int": "42",
    }

    assert property_as_int(properties, "int") == 42
    assert property_as_int(properties, "missing", default=1) == 1
    assert property_as_int(properties, "missing") is None


def test_property_as_int_with_invalid_value() -> None:
    properties = {
        "some_int_prop": "invalid",
    }

    with pytest.raises(ValueError) as exc:
        property_as_int(properties, "some_int_prop")

        assert "Could not parse table property some_int_prop to an integer: invalid" in str(exc.value)


def test_property_as_float() -> None:
    properties = {
        "float": "42.0",
    }

    assert property_as_float(properties, "float", default=1.0) == 42.0
    assert property_as_float(properties, "missing", default=1.0) == 1.0
    assert property_as_float(properties, "missing") is None


def test_property_as_float_with_invalid_value() -> None:
    properties = {
        "some_float_prop": "invalid",
    }

    with pytest.raises(ValueError) as exc:
        property_as_float(properties, "some_float_prop")

        assert "Could not parse table property some_float_prop to a float: invalid" in str(exc.value)


def test_property_as_bool() -> None:
    properties = {
        "bool": "True",
    }

    assert property_as_bool(properties, "bool", default=False) is True
    assert property_as_bool(properties, "missing", default=False) is False
    assert property_as_float(properties, "missing") is None


def test_property_as_bool_with_invalid_value() -> None:
    properties = {
        "some_bool_prop": "invalid",
    }

    with pytest.raises(ValueError) as exc:
        property_as_bool(properties, "some_bool_prop", True)

        assert "Could not parse table property some_bool_prop to a boolean: invalid" in str(exc.value)


def test_get_first_property_value() -> None:
    properties = {
        "prop_1": "value_1",
        "prop_2": "value_2",
    }

    assert get_first_property_value(properties, "prop_2", "prop_1") == "value_2"
    assert get_first_property_value(properties, "missing", "prop_1") == "value_1"
