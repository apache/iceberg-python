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
from typing import Any, Optional

import pytest

from pyiceberg.partitioning import PartitionField, PartitionFieldValue, PartitionKey, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.table.locations import LocationProvider, load_location_provider
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.types import NestedField, StringType

PARTITION_FIELD = PartitionField(source_id=1, field_id=1002, transform=IdentityTransform(), name="string_field")
PARTITION_KEY = PartitionKey(
    field_values=[PartitionFieldValue(PARTITION_FIELD, "example_string")],
    partition_spec=PartitionSpec(PARTITION_FIELD),
    schema=Schema(NestedField(field_id=1, name="string_field", field_type=StringType(), required=False)),
)


class CustomLocationProvider(LocationProvider):
    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        return f"custom_location_provider/{data_file_name}"


def test_simple_location_provider_no_partition() -> None:
    provider = load_location_provider(table_location="table_location", table_properties={"write.object-storage.enabled": "false"})

    assert provider.new_data_location("my_file") == "table_location/data/my_file"


def test_simple_location_provider_with_partition() -> None:
    provider = load_location_provider(table_location="table_location", table_properties={"write.object-storage.enabled": "false"})

    assert provider.new_data_location("my_file", PARTITION_KEY) == "table_location/data/string_field=example_string/my_file"


def test_custom_location_provider() -> None:
    qualified_name = CustomLocationProvider.__module__ + "." + CustomLocationProvider.__name__
    provider = load_location_provider(
        table_location="table_location", table_properties={"write.py-location-provider.impl": qualified_name}
    )

    assert provider.new_data_location("my_file") == "custom_location_provider/my_file"


def test_custom_location_provider_single_path() -> None:
    with pytest.raises(ValueError, match=r"write\.py-location-provider\.impl should be full path"):
        load_location_provider(table_location="table_location", table_properties={"write.py-location-provider.impl": "not_found"})


def test_custom_location_provider_not_found(caplog: Any) -> None:
    with pytest.raises(ValueError, match=r"Could not initialize LocationProvider"):
        load_location_provider(
            table_location="table_location", table_properties={"write.py-location-provider.impl": "module.not_found"}
        )
    assert "ModuleNotFoundError: No module named 'module'" in caplog.text


def test_object_storage_no_partition() -> None:
    provider = load_location_provider(table_location="table_location", table_properties={"write.object-storage.enabled": "true"})

    location = provider.new_data_location("test.parquet")
    parts = location.split("/")

    assert len(parts) == 7
    assert parts[0] == "table_location"
    assert parts[1] == "data"
    assert parts[-1] == "test.parquet"

    # Entropy directories in the middle
    for dir_name in parts[2:-1]:
        assert dir_name
        assert all(c in "01" for c in dir_name)


def test_object_storage_with_partition() -> None:
    provider = load_location_provider(
        table_location="table_location",
        table_properties={"write.object-storage.enabled": "true"},
    )

    location = provider.new_data_location("test.parquet", PARTITION_KEY)

    # Partition values AND entropy included in the path. Entropy differs to that in the test below because the partition
    # key AND the data file name are used as the hash input. This matches Java behaviour; the hash below is what the
    # Java implementation produces for this input too.
    assert location == "table_location/data/0001/0010/1001/00000011/string_field=example_string/test.parquet"


# NB: We test here with None partition key too because disabling partitioned paths still replaces final / with - even in
# paths of un-partitioned files. This matches the behaviour of the Java implementation.
@pytest.mark.parametrize("partition_key", [PARTITION_KEY, None])
def test_object_storage_partitioned_paths_disabled(partition_key: Optional[PartitionKey]) -> None:
    provider = load_location_provider(
        table_location="table_location",
        table_properties={
            "write.object-storage.enabled": "true",
            "write.object-storage.partitioned-paths": "false",
        },
    )

    location = provider.new_data_location("test.parquet", partition_key)

    # No partition values included in the path and last part of entropy is separated with "-"
    assert location == "table_location/data/0110/1010/0011/11101000-test.parquet"


@pytest.mark.parametrize(
    ["data_file_name", "expected_hash"],
    [
        ("a", "0101/0110/1001/10110010"),
        ("b", "1110/0111/1110/00000011"),
        ("c", "0010/1101/0110/01011111"),
        ("d", "1001/0001/0100/01110011"),
    ],
)
def test_hash_injection(data_file_name: str, expected_hash: str) -> None:
    provider = load_location_provider(table_location="table_location", table_properties={"write.object-storage.enabled": "true"})

    assert provider.new_data_location(data_file_name) == f"table_location/data/{expected_hash}/{data_file_name}"


def test_object_location_provider_write_data_path() -> None:
    provider = load_location_provider(
        table_location="s3://table-location/table",
        table_properties={
            "write.object-storage.enabled": "true",
            TableProperties.WRITE_DATA_PATH: "s3://table-location/custom/data/path",
        },
    )

    assert (
        provider.new_data_location("file.parquet") == "s3://table-location/custom/data/path/0010/1111/0101/11011101/file.parquet"
    )


def test_simple_location_provider_write_data_path() -> None:
    provider = load_location_provider(
        table_location="table_location",
        table_properties={
            TableProperties.WRITE_DATA_PATH: "s3://table-location/custom/data/path",
            "write.object-storage.enabled": "false",
        },
    )

    assert provider.new_data_location("file.parquet") == "s3://table-location/custom/data/path/file.parquet"


def test_location_provider_metadata_default_location() -> None:
    provider = load_location_provider(table_location="table_location", table_properties=EMPTY_DICT)

    assert provider.new_metadata_location("manifest.avro") == "table_location/metadata/manifest.avro"


def test_location_provider_metadata_location_with_custom_path() -> None:
    provider = load_location_provider(
        table_location="table_location",
        table_properties={TableProperties.WRITE_METADATA_PATH: "s3://table-location/custom/path"},
    )

    assert provider.new_metadata_location("metadata.json") == "s3://table-location/custom/path/metadata.json"


def test_metadata_location_with_trailing_slash() -> None:
    provider = load_location_provider(
        table_location="table_location",
        table_properties={TableProperties.WRITE_METADATA_PATH: "s3://table-location/custom/path/"},
    )

    assert provider.new_metadata_location("metadata.json") == "s3://table-location/custom/path/metadata.json"
