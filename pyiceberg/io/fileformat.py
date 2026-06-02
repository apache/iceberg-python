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

"""File Format API for writing Iceberg data files."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyiceberg.io import OutputFile
from pyiceberg.manifest import FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec, partition_record_value
from pyiceberg.schema import Schema
from pyiceberg.typedef import Properties, Record

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.io.pyarrow import StatsAggregator


@dataclass(frozen=True)
class DataFileStatistics:
    record_count: int
    column_sizes: dict[int, int]
    value_counts: dict[int, int]
    null_value_counts: dict[int, int]
    nan_value_counts: dict[int, int]
    column_aggregates: dict[int, StatsAggregator]
    split_offsets: list[int]

    def _partition_value(self, partition_field: PartitionField, schema: Schema) -> Any:
        if partition_field.source_id not in self.column_aggregates:
            return None

        source_field = schema.find_field(partition_field.source_id)
        iceberg_transform = partition_field.transform

        if not iceberg_transform.preserves_order:
            raise ValueError(
                f"Cannot infer partition value from parquet metadata for a non-linear Partition Field: "
                f"{partition_field.name} with transform {partition_field.transform}"
            )

        transform_func = iceberg_transform.transform(source_field.field_type)

        lower_value = transform_func(
            partition_record_value(
                partition_field=partition_field,
                value=self.column_aggregates[partition_field.source_id].current_min,
                schema=schema,
            )
        )
        upper_value = transform_func(
            partition_record_value(
                partition_field=partition_field,
                value=self.column_aggregates[partition_field.source_id].current_max,
                schema=schema,
            )
        )
        if lower_value != upper_value:
            raise ValueError(
                f"Cannot infer partition value from parquet metadata as there are more than one partition values "
                f"for Partition Field: {partition_field.name}. {lower_value=}, {upper_value=}"
            )

        return lower_value

    def partition(self, partition_spec: PartitionSpec, schema: Schema) -> Record:
        return Record(*[self._partition_value(field, schema) for field in partition_spec.fields])

    def to_serialized_dict(self) -> dict[str, Any]:
        lower_bounds = {}
        upper_bounds = {}

        for k, agg in self.column_aggregates.items():
            _min = agg.min_as_bytes()
            if _min is not None:
                lower_bounds[k] = _min
            _max = agg.max_as_bytes()
            if _max is not None:
                upper_bounds[k] = _max
        return {
            "record_count": self.record_count,
            "column_sizes": self.column_sizes,
            "value_counts": self.value_counts,
            "null_value_counts": self.null_value_counts,
            "nan_value_counts": self.nan_value_counts,
            "lower_bounds": lower_bounds,
            "upper_bounds": upper_bounds,
            "split_offsets": self.split_offsets,
        }


class FileFormatWriter(ABC):
    """Writes data to a single file in a specific format."""

    _result: DataFileStatistics | None = None

    @abstractmethod
    def write(self, table: pa.Table) -> None:
        """Write a batch of data. May be called multiple times."""

    @abstractmethod
    def close(self) -> DataFileStatistics:
        """Finalize the file and return statistics."""

    def result(self) -> DataFileStatistics:
        """Return statistics from a previous close() call."""
        if self._result is None:
            raise RuntimeError("Writer has not been closed yet")
        return self._result

    def __enter__(self) -> FileFormatWriter:
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager, closing the writer and caching statistics."""
        if exc_type is not None:
            try:
                self.close()
            except Exception:
                pass
            return
        self._result = self.close()


class FileFormatModel(ABC):
    """Represents a file format's capabilities. Creates writers."""

    @property
    @abstractmethod
    def format(self) -> FileFormat: ...

    @abstractmethod
    def file_extension(self) -> str:
        """Return file extension without dot, e.g. 'parquet', 'orc'."""

    @abstractmethod
    def create_writer(
        self,
        output_file: OutputFile,
        file_schema: Schema,
        properties: Properties,
    ) -> FileFormatWriter: ...


class FileFormatFactory:
    """Registry of FileFormatModel implementations."""

    _registry: dict[FileFormat, FileFormatModel] = {}

    @classmethod
    def register(cls, model: FileFormatModel) -> None:
        if model.format in cls._registry:
            existing = cls._registry[model.format]
            raise ValueError(
                f"Cannot register {type(model).__name__}: {type(existing).__name__} is already registered for {model.format}"
            )
        cls._registry[model.format] = model

    @classmethod
    def get(cls, file_format: FileFormat) -> FileFormatModel:
        if file_format not in cls._registry:
            raise ValueError(f"No writer registered for {file_format}. Available: {list(cls._registry.keys())}")
        return cls._registry[file_format]

    @classmethod
    def available_formats(cls) -> list[FileFormat]:
        return list(cls._registry.keys())
