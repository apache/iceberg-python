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

<!-- prettier-ignore-start -->

!!! example "Under development"
    Writing using PyIceberg is still under development. Support for [partial overwrites](https://github.com/apache/iceberg-python/issues/268) and writing to [partitioned tables](https://github.com/apache/iceberg-python/issues/208) is planned and being worked on.

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
