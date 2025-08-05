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

from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)

from pyiceberg.typedef import Properties
from pyiceberg.types import strtobool

HEADER_PREFIX = "header."


def property_as_int(
    properties: Dict[str, str],
    property_name: str,
    default: Optional[int] = None,
) -> Optional[int]:
    if value := properties.get(property_name):
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"Could not parse table property {property_name} to an integer: {value}") from e
    else:
        return default


def property_as_float(
    properties: Dict[str, str],
    property_name: str,
    default: Optional[float] = None,
) -> Optional[float]:
    if value := properties.get(property_name):
        try:
            return float(value)
        except ValueError as e:
            raise ValueError(f"Could not parse table property {property_name} to a float: {value}") from e
    else:
        return default


def property_as_bool(
    properties: Dict[str, str],
    property_name: str,
    default: bool,
) -> bool:
    if value := properties.get(property_name):
        try:
            return strtobool(value)
        except ValueError as e:
            raise ValueError(f"Could not parse table property {property_name} to a boolean: {value}") from e
    return default


def convert_str_to_bool(value: Any) -> bool:
    """Convert string or other value to boolean, handling string representations properly."""
    if isinstance(value, str):
        return strtobool(value)
    return bool(value)


def get_first_property_value(
    properties: Properties,
    *property_names: str,
) -> Optional[Any]:
    for property_name in property_names:
        if property_value := properties.get(property_name):
            return property_value
    return None


def get_first_property_value_with_tracking(props: Properties, used_keys: set[str], *keys: str) -> Optional[Any]:
    """Tracks all candidate keys and returns the first value found."""
    used_keys.update(keys)
    for key in keys:
        if key in props:
            return props[key]
    return None


def get_header_properties(
    properties: Properties,
) -> Properties:
    header_prefix_len = len(HEADER_PREFIX)
    return {key[header_prefix_len:]: value for key, value in properties.items() if key.startswith(HEADER_PREFIX)}


def properties_with_prefix(
    properties: Properties,
    prefix: str,
) -> Properties:
    """
    Return subset of provided map with keys matching the provided prefix. Matching is case-sensitive and the matching prefix is removed from the keys in returned map.

    Args:
        properties: input map
        prefix: prefix to choose keys from input map

    Returns:
        subset of input map with keys starting with provided prefix and prefix trimmed out
    """
    if not properties:
        return {}

    return {key[len(prefix) :]: value for key, value in properties.items() if key.startswith(prefix)}


def filter_properties(
    properties: Properties,
    key_predicate: Callable[[str], bool],
) -> Properties:
    """
    Filter the properties map by the provided key predicate.

    Args:
        properties: input map
        key_predicate: predicate to choose keys from input map

    Returns:
        subset of input map with keys satisfying the predicate
    """
    if not properties:
        return {}

    if key_predicate is None:
        raise ValueError("Invalid key predicate: None")

    return {key: value for key, value in properties.items() if key_predicate(key)}
