---
hide:
  - navigation
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Python API

PyIceberg is based around catalogs to load tables. First step is to instantiate a catalog that loads tables. Let's use the following configuration to define a catalog called `prod`:

```yaml
catalog:
  prod:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret
```

Note that multiple catalogs can be defined in the same `.pyiceberg.yaml`:

```yaml
catalog:
  hive:
    uri: thrift://127.0.0.1:9083
    s3.endpoint: http://127.0.0.1:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
  rest:
    uri: https://rest-server:8181/
    warehouse: my-warehouse
```

and loaded in python by calling `load_catalog(name="hive")` and `load_catalog(name="rest")`.

This information must be placed inside a file called `.pyiceberg.yaml` located either in the `$HOME` or `%USERPROFILE%` directory (depending on whether the operating system is Unix-based or Windows-based, respectively) or in the `$PYICEBERG_HOME` directory (if the corresponding environment variable is set).

For more details on possible configurations refer to the [specific page](https://py.iceberg.apache.org/configuration/).

Then load the `prod` catalog:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "docs",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)
```

Let's create a namespace:

```python
catalog.create_namespace("docs_example")
```

And then list them:

```python
ns = catalog.list_namespaces()

assert ns == [("docs_example",)]
```

And then list tables in the namespace:

```python
catalog.list_tables("docs_example")
```

## Create a table

To create a table from a catalog:

```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=4, name="created_by", field_type=StringType(), required=False
            ),
        ),
        required=False,
    ),
)

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# Sort on the symbol
sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

catalog.create_table(
    identifier="docs_example.bids",
    schema=schema,
    location="s3://pyiceberg",
    partition_spec=partition_spec,
    sort_order=sort_order,
)
```

To create a table using a pyarrow schema:

```python
import pyarrow as pa

schema = pa.schema(
    [
        pa.field("foo", pa.string(), nullable=True),
        pa.field("bar", pa.int32(), nullable=False),
        pa.field("baz", pa.bool_(), nullable=True),
    ]
)

catalog.create_table(
    identifier="docs_example.bids",
    schema=schema,
)
```

To create a table with some subsequent changes atomically in a transaction:

```python
with catalog.create_table_transaction(
    identifier="docs_example.bids",
    schema=schema,
    location="s3://pyiceberg",
    partition_spec=partition_spec,
    sort_order=sort_order,
) as txn:
    with txn.update_schema() as update_schema:
        update_schema.add_column(path="new_column", field_type=StringType())

    with txn.update_spec() as update_spec:
        update_spec.add_identity("symbol")

    txn.set_properties(test_a="test_aa", test_b="test_b", test_c="test_c")
```

## Load a table

### Catalog table

Loading the `bids` table:

```python
table = catalog.load_table("docs_example.bids")
# Equivalent to:
table = catalog.load_table(("docs_example", "bids"))
# The tuple syntax can be used if the namespace or table contains a dot.
```

This returns a `Table` that represents an Iceberg table that can be queried and altered.

### Static table

To load a table directly from a metadata file (i.e., **without** using a catalog), you can use a `StaticTable` as follows:

```python
from pyiceberg.table import StaticTable

static_table = StaticTable.from_metadata(
    "s3://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json"
)
```

The static-table is considered read-only.

## Check if a table exists

To check whether the `bids` table exists:

```python
catalog.table_exists("docs_example.bids")
```

Returns `True` if the table already exists.

## Write support

With PyIceberg 0.6.0 write support is added through Arrow. Let's consider an Arrow Table:

```python
import pyarrow as pa

df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)
```

Next, create a table based on the schema:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

tbl = catalog.create_table("default.cities", schema=schema)
```

Now write the data to the table:

<!-- prettier-ignore-start -->

!!! note inline end "Fast append"
    PyIceberg default to the [fast append](https://iceberg.apache.org/spec/#snapshots) to minimize the amount of data written. This enables quick writes, reducing the possibility of conflicts. The downside of the fast append is that it creates more metadata than a normal commit. [Compaction is planned](https://github.com/apache/iceberg-python/issues/270) and will automatically rewrite all the metadata when a threshold is hit, to maintain performant reads.

<!-- prettier-ignore-end -->

```python
tbl.append(df)

# or

tbl.overwrite(df)
```

The data is written to the table, and when the table is read using `tbl.scan().to_arrow()`:

```
pyarrow.Table
city: string
lat: double
long: double
----
city: [["Amsterdam","San Francisco","Drachten","Paris"]]
lat: [[52.371807,37.773972,53.11254,48.864716]]
long: [[4.896029,-122.431297,6.0989,2.349014]]
```

You both can use `append(df)` or `overwrite(df)` since there is no data yet. If we want to add more data, we can use `.append()` again:

```python
df = pa.Table.from_pylist(
    [{"city": "Groningen", "lat": 53.21917, "long": 6.56667}],
)

tbl.append(df)
```

When reading the table `tbl.scan().to_arrow()` you can see that `Groningen` is now also part of the table:

```
pyarrow.Table
city: string
lat: double
long: double
----
city: [["Amsterdam","San Francisco","Drachten","Paris"],["Groningen"]]
lat: [[52.371807,37.773972,53.11254,48.864716],[53.21917]]
long: [[4.896029,-122.431297,6.0989,2.349014],[6.56667]]
```

The nested lists indicate the different Arrow buffers, where the first write results into a buffer, and the second append in a separate buffer. This is expected since it will read two parquet files.

To avoid any type errors during writing, you can enforce the PyArrow table types using the Iceberg table schema:

```python
from pyiceberg.catalog import load_catalog
import pyarrow as pa

catalog = load_catalog("default")
table = catalog.load_table("default.cities")
schema = table.schema().as_arrow()

df = pa.Table.from_pylist(
    [{"city": "Groningen", "lat": 53.21917, "long": 6.56667}], schema=schema
)

table.append(df)
```

You can delete some of the data from the table by calling `tbl.delete()` with a desired `delete_filter`.

```python
tbl.delete(delete_filter="city == 'Paris'")
```

In the above example, any records where the city field value equals to `Paris` will be deleted.
Running `tbl.scan().to_arrow()` will now yield:

```
pyarrow.Table
city: string
lat: double
long: double
----
city: [["Amsterdam","San Francisco","Drachten"],["Groningen"]]
lat: [[52.371807,37.773972,53.11254],[53.21917]]
long: [[4.896029,-122.431297,6.0989],[6.56667]]
```

## Inspecting tables

To explore the table metadata, tables can be inspected.

<!-- prettier-ignore-start -->

!!! tip "Time Travel"
    To inspect a tables's metadata with the time travel feature, call the inspect table method with the `snapshot_id` argument.
    Time travel is supported on all metadata tables except `snapshots` and `refs`.

    ```python
    table.inspect.entries(snapshot_id=805611270568163028)
    ```

<!-- prettier-ignore-end -->

### Snapshots

Inspect the snapshots of the table:

```python
table.inspect.snapshots()
```

```
pyarrow.Table
committed_at: timestamp[ms] not null
snapshot_id: int64 not null
parent_id: int64
operation: string
manifest_list: string not null
summary: map<string, string>
  child 0, entries: struct<key: string not null, value: string> not null
      child 0, key: string not null
      child 1, value: string
----
committed_at: [[2024-03-15 15:01:25.682,2024-03-15 15:01:25.730,2024-03-15 15:01:25.772]]
snapshot_id: [[805611270568163028,3679426539959220963,5588071473139865870]]
parent_id: [[null,805611270568163028,3679426539959220963]]
operation: [["append","overwrite","append"]]
manifest_list: [["s3://warehouse/default/table_metadata_snapshots/metadata/snap-805611270568163028-0-43637daf-ea4b-4ceb-b096-a60c25481eb5.avro","s3://warehouse/default/table_metadata_snapshots/metadata/snap-3679426539959220963-0-8be81019-adf1-4bb6-a127-e15217bd50b3.avro","s3://warehouse/default/table_metadata_snapshots/metadata/snap-5588071473139865870-0-1382dd7e-5fbc-4c51-9776-a832d7d0984e.avro"]]
summary: [[keys:["added-files-size","added-data-files","added-records","total-data-files","total-delete-files","total-records","total-files-size","total-position-deletes","total-equality-deletes"]values:["5459","1","3","1","0","3","5459","0","0"],keys:["added-files-size","added-data-files","added-records","total-data-files","total-records",...,"total-equality-deletes","total-files-size","deleted-data-files","deleted-records","removed-files-size"]values:["5459","1","3","1","3",...,"0","5459","1","3","5459"],keys:["added-files-size","added-data-files","added-records","total-data-files","total-delete-files","total-records","total-files-size","total-position-deletes","total-equality-deletes"]values:["5459","1","3","2","0","6","10918","0","0"]]]
```

### Partitions

Inspect the partitions of the table:

```python
table.inspect.partitions()
```

```
pyarrow.Table
partition: struct<dt_month: int32, dt_day: date32[day]> not null
  child 0, dt_month: int32
  child 1, dt_day: date32[day]
spec_id: int32 not null
record_count: int64 not null
file_count: int32 not null
total_data_file_size_in_bytes: int64 not null
position_delete_record_count: int64 not null
position_delete_file_count: int32 not null
equality_delete_record_count: int64 not null
equality_delete_file_count: int32 not null
last_updated_at: timestamp[ms]
last_updated_snapshot_id: int64
----
partition: [
  -- is_valid: all not null
  -- child 0 type: int32
[null,null,612]
  -- child 1 type: date32[day]
[null,2021-02-01,null]]
spec_id: [[2,1,0]]
record_count: [[1,1,2]]
file_count: [[1,1,2]]
total_data_file_size_in_bytes: [[641,641,1260]]
position_delete_record_count: [[0,0,0]]
position_delete_file_count: [[0,0,0]]
equality_delete_record_count: [[0,0,0]]
equality_delete_file_count: [[0,0,0]]
last_updated_at: [[2024-04-13 18:59:35.981,2024-04-13 18:59:35.465,2024-04-13 18:59:35.003]]
```

### Entries

To show all the table's current manifest entries for both data and delete files.

```python
table.inspect.entries()
```

```
pyarrow.Table
status: int8 not null
snapshot_id: int64 not null
sequence_number: int64 not null
file_sequence_number: int64 not null
data_file: struct<content: int8 not null, file_path: string not null, file_format: string not null, partition: struct<> not null, record_count: int64 not null, file_size_in_bytes: int64 not null, column_sizes: map<int32, int64>, value_counts: map<int32, int64>, null_value_counts: map<int32, int64>, nan_value_counts: map<int32, int64>, lower_bounds: map<int32, binary>, upper_bounds: map<int32, binary>, key_metadata: binary, split_offsets: list<item: int64>, equality_ids: list<item: int32>, sort_order_id: int32> not null
  child 0, content: int8 not null
  child 1, file_path: string not null
  child 2, file_format: string not null
  child 3, partition: struct<> not null
  child 4, record_count: int64 not null
  child 5, file_size_in_bytes: int64 not null
  child 6, column_sizes: map<int32, int64>
      child 0, entries: struct<key: int32 not null, value: int64> not null
          child 0, key: int32 not null
          child 1, value: int64
  child 7, value_counts: map<int32, int64>
      child 0, entries: struct<key: int32 not null, value: int64> not null
          child 0, key: int32 not null
          child 1, value: int64
  child 8, null_value_counts: map<int32, int64>
      child 0, entries: struct<key: int32 not null, value: int64> not null
          child 0, key: int32 not null
          child 1, value: int64
  child 9, nan_value_counts: map<int32, int64>
      child 0, entries: struct<key: int32 not null, value: int64> not null
          child 0, key: int32 not null
          child 1, value: int64
  child 10, lower_bounds: map<int32, binary>
      child 0, entries: struct<key: int32 not null, value: binary> not null
          child 0, key: int32 not null
          child 1, value: binary
  child 11, upper_bounds: map<int32, binary>
      child 0, entries: struct<key: int32 not null, value: binary> not null
          child 0, key: int32 not null
          child 1, value: binary
  child 12, key_metadata: binary
  child 13, split_offsets: list<item: int64>
      child 0, item: int64
  child 14, equality_ids: list<item: int32>
      child 0, item: int32
  child 15, sort_order_id: int32
readable_metrics: struct<city: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: string, upper_bound: string> not null, lat: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: double, upper_bound: double> not null, long: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: double, upper_bound: double> not null>
  child 0, city: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: string, upper_bound: string> not null
      child 0, column_size: int64
      child 1, value_count: int64
      child 2, null_value_count: int64
      child 3, nan_value_count: int64
      child 4, lower_bound: string
      child 5, upper_bound: string
  child 1, lat: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: double, upper_bound: double> not null
      child 0, column_size: int64
      child 1, value_count: int64
      child 2, null_value_count: int64
      child 3, nan_value_count: int64
      child 4, lower_bound: double
      child 5, upper_bound: double
  child 2, long: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: double, upper_bound: double> not null
      child 0, column_size: int64
      child 1, value_count: int64
      child 2, null_value_count: int64
      child 3, nan_value_count: int64
      child 4, lower_bound: double
      child 5, upper_bound: double
----
status: [[1]]
snapshot_id: [[6245626162224016531]]
sequence_number: [[1]]
file_sequence_number: [[1]]
data_file: [
  -- is_valid: all not null
  -- child 0 type: int8
[0]
  -- child 1 type: string
["s3://warehouse/default/cities/data/00000-0-80766b66-e558-4150-a5cf-85e4c609b9fe.parquet"]
  -- child 2 type: string
["PARQUET"]
  -- child 3 type: struct<>
    -- is_valid: all not null
  -- child 4 type: int64
[4]
  -- child 5 type: int64
[1656]
  -- child 6 type: map<int32, int64>
[keys:[1,2,3]values:[140,135,135]]
  -- child 7 type: map<int32, int64>
[keys:[1,2,3]values:[4,4,4]]
  -- child 8 type: map<int32, int64>
[keys:[1,2,3]values:[0,0,0]]
  -- child 9 type: map<int32, int64>
[keys:[]values:[]]
  -- child 10 type: map<int32, binary>
[keys:[1,2,3]values:[416D7374657264616D,8602B68311E34240,3A77BB5E9A9B5EC0]]
  -- child 11 type: map<int32, binary>
[keys:[1,2,3]values:[53616E204672616E636973636F,F5BEF1B5678E4A40,304CA60A46651840]]
  -- child 12 type: binary
[null]
  -- child 13 type: list<item: int64>
[[4]]
  -- child 14 type: list<item: int32>
[null]
  -- child 15 type: int32
[null]]
readable_metrics: [
  -- is_valid: all not null
  -- child 0 type: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: string, upper_bound: string>
    -- is_valid: all not null
    -- child 0 type: int64
[140]
    -- child 1 type: int64
[4]
    -- child 2 type: int64
[0]
    -- child 3 type: int64
[null]
    -- child 4 type: string
["Amsterdam"]
    -- child 5 type: string
["San Francisco"]
  -- child 1 type: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: double, upper_bound: double>
    -- is_valid: all not null
    -- child 0 type: int64
[135]
    -- child 1 type: int64
[4]
    -- child 2 type: int64
[0]
    -- child 3 type: int64
[null]
    -- child 4 type: double
[37.773972]
    -- child 5 type: double
[53.11254]
  -- child 2 type: struct<column_size: int64, value_count: int64, null_value_count: int64, nan_value_count: int64, lower_bound: double, upper_bound: double>
    -- is_valid: all not null
    -- child 0 type: int64
[135]
    -- child 1 type: int64
[4]
    -- child 2 type: int64
[0]
    -- child 3 type: int64
[null]
    -- child 4 type: double
[-122.431297]
    -- child 5 type: double
[6.0989]]
```

### References

To show a table's known snapshot references:

```python
table.inspect.refs()
```

```
pyarrow.Table
name: string not null
type: string not null
snapshot_id: int64 not null
max_reference_age_in_ms: int64
min_snapshots_to_keep: int32
max_snapshot_age_in_ms: int64
----
name: [["main","testTag"]]
type: [["BRANCH","TAG"]]
snapshot_id: [[2278002651076891950,2278002651076891950]]
max_reference_age_in_ms: [[null,604800000]]
min_snapshots_to_keep: [[null,10]]
max_snapshot_age_in_ms: [[null,604800000]]
```

### Manifests

To show a table's current file manifests:

```python
table.inspect.manifests()
```

```
pyarrow.Table
content: int8 not null
path: string not null
length: int64 not null
partition_spec_id: int32 not null
added_snapshot_id: int64 not null
added_data_files_count: int32 not null
existing_data_files_count: int32 not null
deleted_data_files_count: int32 not null
added_delete_files_count: int32 not null
existing_delete_files_count: int32 not null
deleted_delete_files_count: int32 not null
partition_summaries: list<item: struct<contains_null: bool not null, contains_nan: bool, lower_bound: string, upper_bound: string>> not null
  child 0, item: struct<contains_null: bool not null, contains_nan: bool, lower_bound: string, upper_bound: string>
      child 0, contains_null: bool not null
      child 1, contains_nan: bool
      child 2, lower_bound: string
      child 3, upper_bound: string
----
content: [[0]]
path: [["s3://warehouse/default/table_metadata_manifests/metadata/3bf5b4c6-a7a4-4b43-a6ce-ca2b4887945a-m0.avro"]]
length: [[6886]]
partition_spec_id: [[0]]
added_snapshot_id: [[3815834705531553721]]
added_data_files_count: [[1]]
existing_data_files_count: [[0]]
deleted_data_files_count: [[0]]
added_delete_files_count: [[0]]
existing_delete_files_count: [[0]]
deleted_delete_files_count: [[0]]
partition_summaries: [[    -- is_valid: all not null
    -- child 0 type: bool
[false]
    -- child 1 type: bool
[false]
    -- child 2 type: string
["test"]
    -- child 3 type: string
["test"]]]
```

## Add Files

Expert Iceberg users may choose to commit existing parquet files to the Iceberg table as data files, without rewriting them.

```
# Given that these parquet files have schema consistent with the Iceberg table

file_paths = [
    "s3a://warehouse/default/existing-1.parquet",
    "s3a://warehouse/default/existing-2.parquet",
]

# They can be added to the table without rewriting them

tbl.add_files(file_paths=file_paths)

# A new snapshot is committed to the table with manifests pointing to the existing parquet files
```

<!-- prettier-ignore-start -->

!!! note "Name Mapping"
    Because `add_files` uses existing files without writing new parquet files that are aware of the Iceberg's schema, it requires the Iceberg's table to have a [Name Mapping](https://iceberg.apache.org/spec/?h=name+mapping#name-mapping-serialization) (The Name mapping maps the field names within the parquet files to the Iceberg field IDs). Hence, `add_files` requires that there are no field IDs in the parquet file's metadata, and creates a new Name Mapping based on the table's current schema if the table doesn't already have one.

!!! note "Partitions"
    `add_files` only requires the client to read the existing parquet files' metadata footer to infer the partition value of each file. This implementation also supports adding files to Iceberg tables with partition transforms like `MonthTransform`, and `TruncateTransform` which preserve the order of the values after the transformation (Any Transform that has the `preserves_order` property set to True is supported). Please note that if the column statistics of the `PartitionField`'s source column are not present in the parquet metadata, the partition value is inferred as `None`.

!!! warning "Maintenance Operations"
    Because `add_files` commits the existing parquet files to the Iceberg Table as any other data file, destructive maintenance operations like expiring snapshots will remove them.

<!-- prettier-ignore-end -->

## Schema evolution

PyIceberg supports full schema evolution through the Python API. It takes care of setting the field-IDs and makes sure that only non-breaking changes are done (can be overriden).

In the examples below, the `.update_schema()` is called from the table itself.

```python
with table.update_schema() as update:
    update.add_column("some_field", IntegerType(), "doc")
```

You can also initiate a transaction if you want to make more changes than just evolving the schema:

```python
with table.transaction() as transaction:
    with transaction.update_schema() as update_schema:
        update.add_column("some_other_field", IntegerType(), "doc")
    # ... Update properties etc
```

### Union by Name

Using `.union_by_name()` you can merge another schema into an existing schema without having to worry about field-IDs:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, LongType

catalog = load_catalog()

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

table = catalog.create_table("default.locations", schema)

new_schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
    NestedField(10, "population", LongType(), required=False),
)

with table.update_schema() as update:
    update.union_by_name(new_schema)
```

Now the table has the union of the two schemas `print(table.schema())`:

```
table {
  1: city: optional string
  2: lat: optional double
  3: long: optional double
  4: population: optional long
}
```

### Add column

Using `add_column` you can add a column, without having to worry about the field-id:

```python
with table.update_schema() as update:
    update.add_column("retries", IntegerType(), "Number of retries to place the bid")
    # In a struct
    update.add_column("details.confirmed_by", StringType(), "Name of the exchange")
```

### Rename column

Renaming a field in an Iceberg table is simple:

```python
with table.update_schema() as update:
    update.rename_column("retries", "num_retries")
    # This will rename `confirmed_by` to `exchange`
    update.rename_column("properties.confirmed_by", "exchange")
```

### Move column

Move a field inside of struct:

```python
with table.update_schema() as update:
    update.move_first("symbol")
    update.move_after("bid", "ask")
    # This will move `confirmed_by` before `exchange`
    update.move_before("details.created_by", "details.exchange")
```

### Update column

Update a fields' type, description or required.

```python
with table.update_schema() as update:
    # Promote a float to a double
    update.update_column("bid", field_type=DoubleType())
    # Make a field optional
    update.update_column("symbol", required=False)
    # Update the documentation
    update.update_column("symbol", doc="Name of the share on the exchange")
```

Be careful, some operations are not compatible, but can still be done at your own risk by setting `allow_incompatible_changes`:

```python
with table.update_schema(allow_incompatible_changes=True) as update:
    # Incompatible change, cannot require an optional field
    update.update_column("symbol", required=True)
```

### Delete column

Delete a field, careful this is a incompatible change (readers/writers might expect this field):

```python
with table.update_schema(allow_incompatible_changes=True) as update:
    update.delete_column("some_field")
```

## Partition evolution

PyIceberg supports partition evolution. See the [partition evolution](https://iceberg.apache.org/spec/#partition-evolution)
for more details.

The API to use when evolving partitions is the `update_spec` API on the table.

```python
with table.update_spec() as update:
    update.add_field("id", BucketTransform(16), "bucketed_id")
    update.add_field("event_ts", DayTransform(), "day_ts")
```

Updating the partition spec can also be done as part of a transaction with other operations.

```python
with table.transaction() as transaction:
    with transaction.update_spec() as update_spec:
        update_spec.add_field("id", BucketTransform(16), "bucketed_id")
        update_spec.add_field("event_ts", DayTransform(), "day_ts")
    # ... Update properties etc
```

### Add fields

New partition fields can be added via the `add_field` API which takes in the field name to partition on,
the partition transform, and an optional partition name. If the partition name is not specified,
one will be created.

```python
with table.update_spec() as update:
    update.add_field("id", BucketTransform(16), "bucketed_id")
    update.add_field("event_ts", DayTransform(), "day_ts")
    # identity is a shortcut API for adding an IdentityTransform
    update.identity("some_field")
```

### Remove fields

Partition fields can also be removed via the `remove_field` API if it no longer makes sense to partition on those fields.

```python
with table.update_spec() as update:some_partition_name
    # Remove the partition field with the name
    update.remove_field("some_partition_name")
```

### Rename fields

Partition fields can also be renamed via the `rename_field` API.

```python
with table.update_spec() as update:
    # Rename the partition field with the name bucketed_id to sharded_id
    update.rename_field("bucketed_id", "sharded_id")
```

## Table properties

Set and remove properties through the `Transaction` API:

```python
with table.transaction() as transaction:
    transaction.set_properties(abc="def")

assert table.properties == {"abc": "def"}

with table.transaction() as transaction:
    transaction.remove_properties("abc")

assert table.properties == {}
```

Or, without context manager:

```python
table = table.transaction().set_properties(abc="def").commit_transaction()

assert table.properties == {"abc": "def"}

table = table.transaction().remove_properties("abc").commit_transaction()

assert table.properties == {}
```

## Snapshot properties

Optionally, Snapshot properties can be set while writing to a table using `append` or `overwrite` API:

```python
tbl.append(df, snapshot_properties={"abc": "def"})

# or

tbl.overwrite(df, snapshot_properties={"abc": "def"})

assert tbl.metadata.snapshots[-1].summary["abc"] == "def"
```

## Query the data

To query a table, a table scan is needed. A table scan accepts a filter, columns, optionally a limit and a snapshot ID:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

catalog = load_catalog("default")
table = catalog.load_table("nyc.taxis")

scan = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
    limit=100,
)

# Or filter using a string predicate
scan = table.scan(
    row_filter="trip_distance > 10.0",
)

[task.file.file_path for task in scan.plan_files()]
```

The low level API `plan_files` methods returns a set of tasks that provide the files that might contain matching rows:

```json
[
  "s3://warehouse/wh/nyc/taxis/data/00003-4-42464649-92dd-41ad-b83b-dea1a2fe4b58-00001.parquet"
]
```

In this case it is up to the engine itself to filter the file itself. Below, `to_arrow()` and `to_duckdb()` that already do this for you.

### Apache Arrow

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [`pyarrow` to be installed](index.md).

<!-- prettier-ignore-end -->

Using PyIceberg it is filter out data from a huge table and pull it into a PyArrow table:

```python
table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_arrow()
```

This will return a PyArrow table:

```
pyarrow.Table
VendorID: int64
tpep_pickup_datetime: timestamp[us, tz=+00:00]
tpep_dropoff_datetime: timestamp[us, tz=+00:00]
----
VendorID: [[2,1,2,1,1,...,2,2,2,2,2],[2,1,1,1,2,...,1,1,2,1,2],...,[2,2,2,2,2,...,2,6,6,2,2],[2,2,2,2,2,...,2,2,2,2,2]]
tpep_pickup_datetime: [[2021-04-01 00:28:05.000000,...,2021-04-30 23:44:25.000000]]
tpep_dropoff_datetime: [[2021-04-01 00:47:59.000000,...,2021-05-01 00:14:47.000000]]
```

This will only pull in the files that that might contain matching rows.

### Pandas

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [`pandas` to be installed](index.md).

<!-- prettier-ignore-end -->

PyIceberg makes it easy to filter out data from a huge table and pull it into a Pandas dataframe locally. This will only fetch the relevant Parquet files for the query and apply the filter. This will reduce IO and therefore improve performance and reduce cost.

```python
table.scan(
    row_filter="trip_distance >= 10.0",
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_pandas()
```

This will return a Pandas dataframe:

```
        VendorID      tpep_pickup_datetime     tpep_dropoff_datetime
0              2 2021-04-01 00:28:05+00:00 2021-04-01 00:47:59+00:00
1              1 2021-04-01 00:39:01+00:00 2021-04-01 00:57:39+00:00
2              2 2021-04-01 00:14:42+00:00 2021-04-01 00:42:59+00:00
3              1 2021-04-01 00:17:17+00:00 2021-04-01 00:43:38+00:00
4              1 2021-04-01 00:24:04+00:00 2021-04-01 00:56:20+00:00
...          ...                       ...                       ...
116976         2 2021-04-30 23:56:18+00:00 2021-05-01 00:29:13+00:00
116977         2 2021-04-30 23:07:41+00:00 2021-04-30 23:37:18+00:00
116978         2 2021-04-30 23:38:28+00:00 2021-05-01 00:12:04+00:00
116979         2 2021-04-30 23:33:00+00:00 2021-04-30 23:59:00+00:00
116980         2 2021-04-30 23:44:25+00:00 2021-05-01 00:14:47+00:00

[116981 rows x 3 columns]
```

It is recommended to use Pandas 2 or later, because it stores the data in an [Apache Arrow backend](https://datapythonista.me/blog/pandas-20-and-the-arrow-revolution-part-i) which avoids copies of data.

### DuckDB

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [DuckDB to be installed](index.md).

<!-- prettier-ignore-end -->

A table scan can also be converted into a in-memory DuckDB table:

```python
con = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_duckdb(table_name="distant_taxi_trips")
```

Using the cursor that we can run queries on the DuckDB table:

```python
print(
    con.execute(
        "SELECT tpep_dropoff_datetime - tpep_pickup_datetime AS duration FROM distant_taxi_trips LIMIT 4"
    ).fetchall()
)
[
    (datetime.timedelta(seconds=1194),),
    (datetime.timedelta(seconds=1118),),
    (datetime.timedelta(seconds=1697),),
    (datetime.timedelta(seconds=1581),),
]
```

### Ray

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [Ray to be installed](index.md).

<!-- prettier-ignore-end -->

A table scan can also be converted into a Ray dataset:

```python
ray_dataset = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_ray()
```

This will return a Ray dataset:

```
Dataset(
    num_blocks=1,
    num_rows=1168798,
    schema={
        VendorID: int64,
        tpep_pickup_datetime: timestamp[us, tz=UTC],
        tpep_dropoff_datetime: timestamp[us, tz=UTC]
    }
)
```

Using [Ray Dataset API](https://docs.ray.io/en/latest/data/api/dataset.html) to interact with the dataset:

```python
print(ray_dataset.take(2))
[
    {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime.datetime(2008, 12, 31, 23, 23, 50),
        "tpep_dropoff_datetime": datetime.datetime(2009, 1, 1, 0, 34, 31),
    },
    {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime.datetime(2008, 12, 31, 23, 5, 3),
        "tpep_dropoff_datetime": datetime.datetime(2009, 1, 1, 16, 10, 18),
    },
]
```

### Daft

PyIceberg interfaces closely with Daft Dataframes (see also: [Daft integration with Iceberg](https://www.getdaft.io/projects/docs/en/latest/user_guide/integrations/iceberg.html)) which provides a full lazily optimized query engine interface on top of PyIceberg tables.

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [Daft to be installed](index.md).

<!-- prettier-ignore-end -->

A table can be read easily into a Daft Dataframe:

```python
df = table.to_daft()  # equivalent to `daft.read_iceberg(table)`
df = df.where(df["trip_distance"] >= 10.0)
df = df.select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime")
```

This returns a Daft Dataframe which is lazily materialized. Printing `df` will display the schema:

```
╭──────────┬───────────────────────────────┬───────────────────────────────╮
│ VendorID ┆ tpep_pickup_datetime          ┆ tpep_dropoff_datetime         │
│ ---      ┆ ---                           ┆ ---                           │
│ Int64    ┆ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
╰──────────┴───────────────────────────────┴───────────────────────────────╯

(No data to display: Dataframe not materialized)
```

We can execute the Dataframe to preview the first few rows of the query with `df.show()`.

This is correctly optimized to take advantage of Iceberg features such as hidden partitioning and file-level statistics for efficient reads.

```python
df.show(2)
```

```
╭──────────┬───────────────────────────────┬───────────────────────────────╮
│ VendorID ┆ tpep_pickup_datetime          ┆ tpep_dropoff_datetime         │
│ ---      ┆ ---                           ┆ ---                           │
│ Int64    ┆ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
╞══════════╪═══════════════════════════════╪═══════════════════════════════╡
│ 2        ┆ 2008-12-31T23:23:50.000000    ┆ 2009-01-01T00:34:31.000000    │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2        ┆ 2008-12-31T23:05:03.000000    ┆ 2009-01-01T16:10:18.000000    │
╰──────────┴───────────────────────────────┴───────────────────────────────╯

(Showing first 2 rows)
```
