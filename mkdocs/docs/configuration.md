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

# Configuration

## Setting Configuration Values

There are three ways to pass in configuration:

- Using the `.pyiceberg.yaml` configuration file (Recommended)
- Through environment variables
- By passing in credentials through the CLI or the Python API

The configuration file can be stored in either the directory specified by the `PYICEBERG_HOME` environment variable, the home directory, or current working directory (in this order).

To change the path searched for the `.pyiceberg.yaml`, you can overwrite the `PYICEBERG_HOME` environment variable.

Another option is through environment variables:

```sh
export PYICEBERG_CATALOG__DEFAULT__URI=thrift://localhost:9083
export PYICEBERG_CATALOG__DEFAULT__S3__ACCESS_KEY_ID=username
export PYICEBERG_CATALOG__DEFAULT__S3__SECRET_ACCESS_KEY=password
```

The environment variable picked up by Iceberg starts with `PYICEBERG_` and then follows the yaml structure below, where a double underscore `__` represents a nested field, and the underscore `_` is converted into a dash `-`.

For example, `PYICEBERG_CATALOG__DEFAULT__S3__ACCESS_KEY_ID`, sets `s3.access-key-id` on the `default` catalog.

## Tables

Iceberg tables support table properties to configure table behavior.

### Write options

| Key                                      | Options                            | Default                    | Description                                                                                                                                          |
|------------------------------------------|------------------------------------|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `write.parquet.compression-codec`        | `{uncompressed,zstd,gzip,snappy}`  | zstd                       | Sets the Parquet compression coddec.                                                                                                                 |
| `write.parquet.compression-level`        | Integer                            | null                       | Parquet compression level for the codec. If not set, it is up to PyIceberg                                                                           |
| `write.parquet.row-group-limit`          | Number of rows                     | 1048576                    | The upper bound of the number of entries within a single row group                                                                                   |
| `write.parquet.page-size-bytes`          | Size in bytes                      | 1MB                        | Set a target threshold for the approximate encoded size of data pages within a column chunk                                                          |
| `write.parquet.page-row-limit`           | Number of rows                     | 20000                      | Set a target threshold for the maximum number of rows within a column chunk                                                                          |
| `write.parquet.dict-size-bytes`          | Size in bytes                      | 2MB                        | Set the dictionary page size limit per row group                                                                                                     |
| `write.metadata.previous-versions-max`   | Integer                            | 100                        | The max number of previous version metadata files to keep before deleting after commit.                                                              |
| `write.metadata.delete-after-commit.enabled` | Boolean                        | False                      | Whether to automatically delete old *tracked* metadata files after each table commit. It will retain a number of the most recent metadata files, which can be set using property `write.metadata.previous-versions-max`. |
| `write.object-storage.enabled`           | Boolean                            | False                      | Enables the [`ObjectStoreLocationProvider`](configuration.md#object-store-location-provider) that adds a hash component to file paths. |
| `write.object-storage.partitioned-paths` | Boolean                            | True                       | Controls whether [partition values are included in file paths](configuration.md#partition-exclusion) when object storage is enabled                  |
| `write.py-location-provider.impl`        | String of form `module.ClassName`  | null                       | Optional, [custom `LocationProvider`](configuration.md#loading-a-custom-location-provider) implementation                                            |
| `write.data.path`                        | String pointing to location        | `{metadata.location}/data` | Sets the location under which data is written.                                                                                                       |
| `write.metadata.path`                    | String pointing to location        | `{metadata.location}/metadata` | Sets the location under which metadata is written.                                                                                                                                                                  |

### Table behavior options

| Key                                  | Options             | Default       | Description                                                 |
| ------------------------------------ | ------------------- | ------------- | ----------------------------------------------------------- |
| `commit.manifest.target-size-bytes`  | Size in bytes       | 8388608 (8MB) | Target size when merging manifest files                     |
| `commit.manifest.min-count-to-merge` | Number of manifests | 100           | Target size when merging manifest files                     |
| `commit.manifest-merge.enabled`      | Boolean             | False         | Controls whether to automatically merge manifests on writes |

<!-- prettier-ignore-start -->

!!! note "Fast append"
    Unlike Java implementation, PyIceberg default to the [fast append](api.md#write-support) and thus `commit.manifest-merge.enabled` is set to `False` by default.

<!-- prettier-ignore-end -->

## FileIO

Iceberg works with the concept of a FileIO which is a pluggable module for reading, writing, and deleting files. By default, PyIceberg will try to initialize the FileIO that's suitable for the scheme (`s3://`, `gs://`, etc.) and will use the first one that's installed.

- **s3**, **s3a**, **s3n**: `PyArrowFileIO`, `FsspecFileIO`
- **gs**: `PyArrowFileIO`
- **file**: `PyArrowFileIO`
- **hdfs**: `PyArrowFileIO`
- **abfs**, **abfss**: `FsspecFileIO`
- **oss**: `PyArrowFileIO`
- **hf**: `FsspecFileIO`

You can also set the FileIO explicitly:

| Key        | Example                          | Description                                                                                     |
| ---------- | -------------------------------- | ----------------------------------------------------------------------------------------------- |
| py-io-impl | pyiceberg.io.fsspec.FsspecFileIO | Sets the FileIO explicitly to an implementation, and will fail explicitly if it can't be loaded |

For the FileIO there are several configuration options available:

### S3

<!-- markdown-link-check-disable -->

| Key                         | Example                    | Description                                                                                                                                                                                                                                                             |
|-----------------------------|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| s3.endpoint                 | <https://10.0.19.25/>      | Configure an alternative endpoint of the S3 service for the FileIO to access. This could be used to use S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud.               |
| s3.access-key-id            | admin                      | Configure the static access key id used to access the FileIO.                                                                                                                                                                                                           |
| s3.secret-access-key        | password                   | Configure the static secret access key used to access the FileIO.                                                                                                                                                                                                       |
| s3.session-token            | AQoDYXdzEJr...             | Configure the static session token used to access the FileIO.                                                                                                                                                                                                           |
| s3.role-session-name        | session                    | An optional identifier for the assumed role session.                                                                                                                                                                                                                    |
| s3.role-arn                 | arn:aws:...                | AWS Role ARN. If provided instead of access_key and secret_key, temporary credentials will be fetched by assuming this role.                                                                                                                                            |
| s3.signer                   | bearer                     | Configure the signature version of the FileIO.                                                                                                                                                                                                                          |
| s3.signer.uri               | <http://my.signer:8080/s3> | Configure the remote signing uri if it differs from the catalog uri. Remote signing is only implemented for `FsspecFileIO`. The final request is sent to `<s3.signer.uri>/<s3.signer.endpoint>`.                                                                        |
| s3.signer.endpoint          | v1/main/s3-sign            | Configure the remote signing endpoint. Remote signing is only implemented for `FsspecFileIO`. The final request is sent to `<s3.signer.uri>/<s3.signer.endpoint>`. (default : v1/aws/s3/sign).                                                                          |
| s3.region                   | us-west-2                  | Configure the default region used to initialize an `S3FileSystem`. `PyArrowFileIO` attempts to automatically tries to resolve the region if this isn't set (only supported for AWS S3 Buckets).                                                                         |
| s3.resolve-region           | False                      | Only supported for `PyArrowFileIO`, when enabled, it will always try to resolve the location of the bucket (only supported for AWS S3 Buckets).                                                                                                                         |
| s3.proxy-uri                | <http://my.proxy.com:8080> | Configure the proxy server to be used by the FileIO.                                                                                                                                                                                                                    |
| s3.connect-timeout          | 60.0                       | Configure socket connection timeout, in seconds.                                                                                                                                                                                                                        |
| s3.request-timeout          | 60.0                       | Configure socket read timeouts on Windows and macOS, in seconds.                                                                                                                                                                                                        |
| s3.force-virtual-addressing | False                      | Whether to use virtual addressing of buckets. If true, then virtual addressing is always enabled. If false, then virtual addressing is only enabled if endpoint_override is empty. This can be used for non-AWS backends that only support virtual hosted-style access. |

<!-- markdown-link-check-enable-->

### HDFS

<!-- markdown-link-check-disable -->

| Key                  | Example             | Description                                      |
| -------------------- | ------------------- | ------------------------------------------------ |
| hdfs.host            | <https://10.0.19.25/> | Configure the HDFS host to connect to            |
| hdfs.port            | 9000                | Configure the HDFS port to connect to.           |
| hdfs.user            | user                | Configure the HDFS username used for connection. |
| hdfs.kerberos_ticket | kerberos_ticket     | Configure the path to the Kerberos ticket cache. |

<!-- markdown-link-check-enable-->

### Azure Data lake

<!-- markdown-link-check-disable -->

| Key                    | Example                                                                                   | Description                                                                                                                                                                                                                                                                            |
| ---------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| adls.connection-string | AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqF...;BlobEndpoint=<http://localhost/> | A [connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string). This could be used to use FileIO with any adls-compatible object storage service that has a different endpoint (like [azurite](https://github.com/azure/azurite)). |
| adls.account-name      | devstoreaccount1                                                                          | The account that you want to connect to                                                                                                                                                                                                                                                |
| adls.account-key       | Eby8vdM02xNOcqF...                                                                        | The key to authentication against the account.                                                                                                                                                                                                                                         |
| adls.sas-token         | NuHOuuzdQN7VRM%2FOpOeqBlawRCA845IY05h9eu1Yte4%3D                                          | The shared access signature                                                                                                                                                                                                                                                            |
| adls.tenant-id         | ad667be4-b811-11ed-afa1-0242ac120002                                                      | The tenant-id                                                                                                                                                                                                                                                                          |
| adls.client-id         | ad667be4-b811-11ed-afa1-0242ac120002                                                      | The client-id                                                                                                                                                                                                                                                                          |
| adls.client-secret     | oCA3R6P\*ka#oa1Sms2J74z...                                                                | The client-secret                                                                                                                                                                                                                                                                      |
| adls.account-host      | accountname1.blob.core.windows.net                                                        | The storage account host. See [AzureBlobFileSystem](https://github.com/fsspec/adlfs/blob/adb9c53b74a0d420625b86dd00fbe615b43201d2/adlfs/spec.py#L125) for reference                                                                                                                   |

<!-- markdown-link-check-enable-->

### Google Cloud Storage

<!-- markdown-link-check-disable -->

| Key                         | Example             | Description                                                                                                                                                                                                                                         |
| --------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| gcs.project-id              | my-gcp-project      | Configure Google Cloud Project for GCS FileIO.                                                                                                                                                                                                      |
| gcs.oauth2.token            | ya29.dr.AfM...      | String representation of the access token used for temporary access.                                                                                                                                                                                |
| gcs.oauth2.token-expires-at | 1690971805918       | Configure expiration for credential generated with an access token. Milliseconds since epoch                                                                                                                                                        |
| gcs.access                  | read_only           | Configure client to have specific access. Must be one of 'read_only', 'read_write', or 'full_control'                                                                                                                                               |
| gcs.consistency             | md5                 | Configure the check method when writing files. Must be one of 'none', 'size', or 'md5'                                                                                                                                                              |
| gcs.cache-timeout           | 60                  | Configure the cache expiration time in seconds for object metadata cache                                                                                                                                                                            |
| gcs.requester-pays          | False               | Configure whether to use requester-pays requests                                                                                                                                                                                                    |
| gcs.session-kwargs          | {}                  | Configure a dict of parameters to pass on to aiohttp.ClientSession; can contain, for example, proxy settings.                                                                                                                                       |
| gcs.service.host            | <http://0.0.0.0:4443> | Configure an alternative endpoint for the GCS FileIO to access (format protocol://host:port) If not given, defaults to the value of environment variable "STORAGE_EMULATOR_HOST"; if that is not set either, will use the standard Google endpoint. |
| gcs.default-location        | US                  | Configure the default location where buckets are created, like 'US' or 'EUROPE-WEST3'.                                                                                                                                                              |
| gcs.version-aware           | False               | Configure whether to support object versioning on the GCS bucket.                                                                                                                                                                                   |

<!-- markdown-link-check-enable-->

### Alibaba Cloud Object Storage Service (OSS)

<!-- markdown-link-check-disable -->

PyIceberg uses [S3FileSystem](https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html) class to connect to OSS bucket as the service is [compatible with S3 SDK](https://www.alibabacloud.com/help/en/oss/developer-reference/use-amazon-s3-sdks-to-access-oss) as long as the endpoint is addressed with virtual hosted style.

| Key                  | Example             | Description                                      |
| -------------------- | ------------------- | ------------------------------------------------ |
| s3.endpoint          | <https://s3.oss-your-bucket-region.aliyuncs.com/>      | Configure an endpoint of the OSS service for the FileIO to access. Be sure to use S3 compatible endpoint as given in the example. |
| s3.access-key-id     | admin                      | Configure the static access key id used to access the FileIO.                                                                                                                                                                                             |
| s3.secret-access-key | password                   | Configure the static secret access key used to access the FileIO.                                                                                                                                                                                         |
| s3.session-token     | AQoDYXdzEJr...             | Configure the static session token used to access the FileIO.                                                                                                                                                                                             |
| s3.force-virtual-addressing   | True                       | Whether to use virtual addressing of buckets. This is set to `True` by default as OSS can only be accessed with virtual hosted style address.                                                                                                                                                                                                        |

<!-- markdown-link-check-enable-->

### Hugging Face

<!-- markdown-link-check-disable -->

| Key         | Example                  | Description                                               |
| ----------- | ------------------------ | --------------------------------------------------------- |
| hf.endpoint | <https://huggingface.co> | Configure the endpoint for Hugging Face                   |
| hf.token    | hf_xxx                   | The Hugging Face token to access HF Datasets repositories |

<!-- markdown-link-check-enable-->

### PyArrow

<!-- markdown-link-check-disable -->

| Key                             | Example | Description                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| pyarrow.use-large-types-on-read | True    | Use large PyArrow types i.e. [large_string](https://arrow.apache.org/docs/python/generated/pyarrow.large_string.html), [large_binary](https://arrow.apache.org/docs/python/generated/pyarrow.large_binary.html) and [large_list](https://arrow.apache.org/docs/python/generated/pyarrow.large_list.html) field types on table scans. The default value is True. |

<!-- markdown-link-check-enable-->

## Location Providers

Apache Iceberg uses the concept of a `LocationProvider` to manage file paths for a table's data files. In PyIceberg, the
`LocationProvider` module is designed to be pluggable, allowing customization for specific use cases, and to additionally determine metadata file locations. The
`LocationProvider` for a table can be specified through table properties.

Both data file and metadata file locations can be customized by configuring the table properties [`write.data.path` and `write.metadata.path`](#write-options), respectively.

For more granular control, you can override the `LocationProvider`'s `new_data_location` and `new_metadata_location` methods to define custom logic for generating file paths. See [`Loading a Custom Location Provider`](configuration.md#loading-a-custom-location-provider).

PyIceberg defaults to the [`SimpleLocationProvider`](configuration.md#simple-location-provider) for managing file paths.

### Simple Location Provider

The `SimpleLocationProvider` provides paths prefixed by `{location}/data/`, where `location` comes from the [table metadata](https://iceberg.apache.org/spec/#table-metadata-fields). This can be overridden by setting [`write.data.path` table configuration](#write-options).

For example, a non-partitioned table might have a data file with location:

```txt
s3://bucket/ns/table/data/0000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

When the table is partitioned, files under a given partition are grouped into a subdirectory, with that partition key
and value as the directory name - this is known as the *Hive-style* partition path format. For example, a table
partitioned over a string column `category` might have a data file with location:

```txt
s3://bucket/ns/table/data/category=orders/0000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

### Object Store Location Provider

PyIceberg offers the `ObjectStoreLocationProvider`, and an optional [partition-exclusion](configuration.md#partition-exclusion)
optimization, designed for tables stored in object storage. For additional context and motivation concerning these configurations,
see their [documentation for Iceberg's Java implementation](https://iceberg.apache.org/docs/latest/aws/#object-store-file-layout).

When several files are stored under the same prefix, cloud object stores such as S3 often [throttle requests on prefixes](https://repost.aws/knowledge-center/http-5xx-errors-s3),
resulting in slowdowns. The `ObjectStoreLocationProvider` counteracts this by injecting deterministic hashes, in the form of binary directories,
into file paths, to distribute files across a larger number of object store prefixes.

Paths are prefixed by `{location}/data/`, where `location` comes from the [table metadata](https://iceberg.apache.org/spec/#table-metadata-fields), in a similar manner to the [`SimpleLocationProvider`](configuration.md#simple-location-provider). This can be overridden by setting [`write.data.path` table configuration](#write-options).

For example, a table partitioned over a string column `category` might have a data file with location: (note the additional binary directories)

```txt
s3://bucket/ns/table/data/0101/0110/1001/10110010/category=orders/0000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

The `ObjectStoreLocationProvider` is enabled for a table by explicitly setting its `write.object-storage.enabled` table
property to `True`.

#### Partition Exclusion

When the `ObjectStoreLocationProvider` is used, the table property `write.object-storage.partitioned-paths`, which
defaults to `True`, can be set to `False` as an additional optimization for object stores. This omits partition keys and
values from data file paths *entirely* to further reduce key size. With it disabled, the same data file above would
instead be written to: (note the absence of `category=orders`)

```txt
s3://bucket/ns/table/data/1101/0100/1011/00111010-00000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

### Loading a Custom Location Provider

Similar to FileIO, a custom `LocationProvider` may be provided for a table by concretely subclassing the abstract base
class [`LocationProvider`](../reference/pyiceberg/table/locations/#pyiceberg.table.locations.LocationProvider).

The table property `write.py-location-provider.impl` should be set to the fully-qualified name of the custom
`LocationProvider` (i.e. `mymodule.MyLocationProvider`). Recall that a `LocationProvider` is configured per-table,
permitting different location provision for different tables. Note also that Iceberg's Java implementation uses a
different table property, `write.location-provider.impl`, for custom Java implementations.

An example, custom `LocationProvider` implementation is shown below.

```py
import uuid

class UUIDLocationProvider(LocationProvider):
    def __init__(self, table_location: str, table_properties: Properties):
        super().__init__(table_location, table_properties)

    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        # Can use any custom method to generate a file path given the partitioning information and file name
        prefix = f"{self.table_location}/{uuid.uuid4()}"
        return f"{prefix}/{partition_key.to_path()}/{data_file_name}" if partition_key else f"{prefix}/{data_file_name}"
```

## Catalogs

PyIceberg currently has native catalog type support for REST, SQL, Hive, Glue and DynamoDB.
Alternatively, you can also directly set the catalog implementation:

| Key             | Example                      | Description                                                                                      |
| --------------- | ---------------------------- | ------------------------------------------------------------------------------------------------ |
| type            | rest                         | Type of catalog, one of `rest`, `sql`, `hive`, `glue`, `dymamodb`. Default to `rest`             |
| py-catalog-impl | mypackage.mymodule.MyCatalog | Sets the catalog explicitly to an implementation, and will fail explicitly if it can't be loaded |

### REST Catalog

```yaml
catalog:
  default:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret

  default-mtls-secured-catalog:
    uri: https://rest-catalog/ws/
    ssl:
      client:
        cert: /absolute/path/to/client.crt
        key: /absolute/path/to/client.key
      cabundle: /absolute/path/to/cabundle.pem
```

<!-- markdown-link-check-disable -->

| Key                 | Example                          | Description                                                                                        |
| ------------------- | -------------------------------- | -------------------------------------------------------------------------------------------------- |
| uri                 | <https://rest-catalog/ws>          | URI identifying the REST Server                                                                    |
| ugi                 | t-1234:secret                    | Hadoop UGI for Hive client.                                                                        |
| credential          | t-1234:secret                    | Credential to use for OAuth2 credential flow when initializing the catalog                         |
| token               | FEW23.DFSDF.FSDF                 | Bearer token value to use for `Authorization` header                                               |
| scope               | openid offline corpds:ds:profile | Desired scope of the requested security token (default : catalog)                                  |
| resource            | rest_catalog.iceberg.com         | URI for the target resource or service                                                             |
| audience            | rest_catalog                     | Logical name of target resource or service                                                         |
| rest.sigv4-enabled  | true                             | Sign requests to the REST Server using AWS SigV4 protocol                                          |
| rest.signing-region | us-east-1                        | The region to use when SigV4 signing a request                                                     |
| rest.signing-name   | execute-api                      | The service signing name to use when SigV4 signing a request                                       |
| oauth2-server-uri   | <https://auth-service/cc>          | Authentication URL to use for client credentials authentication (default: uri + 'v1/oauth/tokens') |
| snapshot-loading-mode | refs                             | The snapshots to return in the body of the metadata. Setting the value to `all` would return the full set of snapshots currently valid for the table. Setting the value to `refs` would load all snapshots referenced by branches or tags. |

<!-- markdown-link-check-enable-->

#### Headers in RESTCatalog

To configure custom headers in RESTCatalog, include them in the catalog properties with the prefix `header.`. This
ensures that all HTTP requests to the REST service include the specified headers.

```yaml
catalog:
  default:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret
    header.content-type: application/vnd.api+json
```

Specific headers defined by the RESTCatalog spec include:

| Key                                  | Options                               | Default              | Description                                                                                                                                                                                        |
| ------------------------------------ | ------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `header.X-Iceberg-Access-Delegation` | `{vended-credentials,remote-signing}` | `vended-credentials` | Signal to the server that the client supports delegated access via a comma-separated list of access mechanisms. The server may choose to supply access via any or none of the requested mechanisms |

### SQL Catalog

The SQL catalog requires a database for its backend. PyIceberg supports PostgreSQL and SQLite through psycopg2. The database connection has to be configured using the `uri` property. The init_catalog_tables is optional and defaults to True. If it is set to False, the catalog tables will not be created when the SQLCatalog is initialized. See SQLAlchemy's [documentation for URL format](https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls):

For PostgreSQL:

```yaml
catalog:
  default:
    type: sql
    uri: postgresql+psycopg2://username:password@localhost/mydatabase
    init_catalog_tables: false
```

In the case of SQLite:

<!-- prettier-ignore-start -->

!!! warning inline end "Development only"
    SQLite is not built for concurrency, you should use this catalog for exploratory or development purposes.

<!-- prettier-ignore-end -->

```yaml
catalog:
  default:
    type: sql
    uri: sqlite:////tmp/pyiceberg.db
    init_catalog_tables: false
```

| Key           | Example                                                      | Default | Description                                                                                                                                                                                    |
| ------------- | ------------------------------------------------------------ | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| uri           | postgresql+psycopg2://username:password@localhost/mydatabase |         | SQLAlchemy backend URL for the catalog database (see [documentation for URL format](https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls))                                |
| echo          | true                                                         | false   | SQLAlchemy engine [echo param](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.echo) to log all statements to the default log handler                      |
| pool_pre_ping | true                                                         | false   | SQLAlchemy engine [pool_pre_ping param](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_pre_ping) to test connections for liveness upon each checkout |

### In Memory Catalog

The in-memory catalog is built on top of `SqlCatalog` and uses SQLite in-memory database for its backend.

It is useful for test, demo, and playground but not in production as it does not support concurrent access.

```yaml
catalog:
  default:
    type: in-memory
    warehouse: /tmp/pyiceberg/warehouse
```

| Key       | Example                  | Default                       | Description                                                          |
| --------- |--------------------------|-------------------------------|----------------------------------------------------------------------|
| warehouse | /tmp/pyiceberg/warehouse | file:///tmp/iceberg/warehouse | The directory where the in-memory catalog will store its data files. |

### Hive Catalog

```yaml
catalog:
  default:
    uri: thrift://localhost:9083
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

| Key                          | Example | Description                       |
|------------------------------| ------- | --------------------------------- |
| hive.hive2-compatible        | true    | Using Hive 2.x compatibility mode |
| hive.kerberos-authentication | true    | Using authentication via Kerberos |

When using Hive 2.x, make sure to set the compatibility flag:

```yaml
catalog:
  default:
...
    hive.hive2-compatible: true
```

### Glue Catalog

Your AWS credentials can be passed directly through the Python API.
Otherwise, please refer to
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) to set your AWS account credentials locally.

```yaml
catalog:
  default:
    type: glue
    glue.access-key-id: <ACCESS_KEY_ID>
    glue.secret-access-key: <SECRET_ACCESS_KEY>
    glue.session-token: <SESSION_TOKEN>
    glue.region: <REGION_NAME>
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

```yaml
catalog:
  default:
    type: glue
    glue.profile-name: <PROFILE_NAME>
    glue.region: <REGION_NAME>
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

<!-- prettier-ignore-start -->

!!! Note "Client-specific Properties"
    `glue.*` properties are for Glue Catalog only. If you want to use the same credentials for both Glue Catalog and S3 FileIO, you can set the `client.*` properties. See the [Unified AWS Credentials](configuration.md#unified-aws-credentials) section for more details.

<!-- prettier-ignore-end -->

<!-- markdown-link-check-disable -->

| Key                    | Example                                | Description                                                                     |
|------------------------|----------------------------------------|---------------------------------------------------------------------------------|
| glue.id                | 111111111111                           | Configure the 12-digit ID of the Glue Catalog                                   |
| glue.skip-archive      | true                                   | Configure whether to skip the archival of older table versions. Default to true |
| glue.endpoint          | <https://glue.us-east-1.amazonaws.com> | Configure an alternative endpoint of the Glue service for GlueCatalog to access |
| glue.profile-name      | default                                | Configure the static profile used to access the Glue Catalog                    |
| glue.region            | us-east-1                              | Set the region of the Glue Catalog                                              |
| glue.access-key-id     | admin                                  | Configure the static access key id used to access the Glue Catalog              |
| glue.secret-access-key | password                               | Configure the static secret access key used to access the Glue Catalog          |
| glue.session-token     | AQoDYXdzEJr...                         | Configure the static session token used to access the Glue Catalog              |
| glue.max-retries       | 10                                     | Configure the maximum number of retries for the Glue service calls              |
| glue.retry-mode        | standard                               | Configure the retry mode for the Glue service. Default to standard.             |

<!-- markdown-link-check-enable-->

<!-- prettier-ignore-start -->

!!! warning "Removed Properties"
    The properties `profile_name`, `region_name`, `aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token` were deprecated and removed in 0.8.0

<!-- prettier-ignore-end -->

### DynamoDB Catalog

If you want to use AWS DynamoDB as the catalog, you can use the last two ways to configure the pyiceberg and refer
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
to set your AWS account credentials locally.
If you want to use the same credentials for both Dynamodb Catalog and S3 FileIO, you can set the [`client.*` properties](configuration.md#unified-aws-credentials).

```yaml
catalog:
  default:
    type: dynamodb
    table-name: iceberg
```

If you prefer to pass the credentials explicitly to the client instead of relying on environment variables,

```yaml
catalog:
  default:
    type: dynamodb
    table-name: iceberg
    dynamodb.access-key-id: <ACCESS_KEY_ID>
    dynamodb.secret-access-key: <SECRET_ACCESS_KEY>
    dynamodb.session-token: <SESSION_TOKEN>
    dynamodb.region: <REGION_NAME>
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

<!-- prettier-ignore-start -->

!!! Note "Client-specific Properties"
    `dynamodb.*` properties are for DynamoDB Catalog only. If you want to use the same credentials for both DynamoDB Catalog and S3 FileIO, you can set the `client.*` properties. See the [Unified AWS Credentials](configuration.md#unified-aws-credentials) section for more details.

<!-- prettier-ignore-end -->

<!-- markdown-link-check-disable -->

| Key                        | Example        | Description                                                                |
| -------------------------- | -------------- | -------------------------------------------------------------------------- |
| dynamodb.profile-name      | default        | Configure the static profile used to access the DynamoDB Catalog           |
| dynamodb.region            | us-east-1      | Set the region of the DynamoDB Catalog                                     |
| dynamodb.access-key-id     | admin          | Configure the static access key id used to access the DynamoDB Catalog     |
| dynamodb.secret-access-key | password       | Configure the static secret access key used to access the DynamoDB Catalog |
| dynamodb.session-token     | AQoDYXdzEJr... | Configure the static session token used to access the DynamoDB Catalog     |

<!-- markdown-link-check-enable-->

<!-- prettier-ignore-start -->

!!! warning "Removed Properties"
    The properties `profile_name`, `region_name`, `aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token` were deprecated and removed in 0.8.0

<!-- prettier-ignore-end -->

### Custom Catalog Implementations

If you want to load any custom catalog implementation, you can set catalog configurations like the following:

```yaml
catalog:
  default:
    py-catalog-impl: mypackage.mymodule.MyCatalog
    custom-key1: value1
    custom-key2: value2
```

## Unified AWS Credentials

You can explicitly set the AWS credentials for both Glue/DynamoDB Catalog and S3 FileIO by configuring `client.*` properties. For example:

```yaml
catalog:
  default:
    type: glue
    client.access-key-id: <ACCESS_KEY_ID>
    client.secret-access-key: <SECRET_ACCESS_KEY>
    client.region: <REGION_NAME>
```

configures the AWS credentials for both Glue Catalog and S3 FileIO.

| Key                      | Example        | Description                                                                                            |
| ------------------------ | -------------- | ------------------------------------------------------------------------------------------------------ |
| client.region            | us-east-1      | Set the region of both the Glue/DynamoDB Catalog and the S3 FileIO                                     |
| client.access-key-id     | admin          | Configure the static access key id used to access both the Glue/DynamoDB Catalog and the S3 FileIO     |
| client.secret-access-key | password       | Configure the static secret access key used to access both the Glue/DynamoDB Catalog and the S3 FileIO |
| client.session-token     | AQoDYXdzEJr... | Configure the static session token used to access both the Glue/DynamoDB Catalog and the S3 FileIO     |
| client.role-session-name      | session                    | An optional identifier for the assumed role session.                                                                                                                                                                                                      |
| client.role-arn          | arn:aws:...                | AWS Role ARN. If provided instead of access_key and secret_key, temporary credentials will be fetched by assuming this role.                                                                                                                              |

<!-- prettier-ignore-start -->

!!! Note "Properties Priority"
    `client.*` properties will be overridden by service-specific properties if they are set. For example, if `client.region` is set to `us-west-1` and `s3.region` is set to `us-east-1`, the S3 FileIO will use `us-east-1` as the region.

<!-- prettier-ignore-end -->

## Concurrency

PyIceberg uses multiple threads to parallelize operations. The number of workers can be configured by supplying a `max-workers` entry in the configuration file, or by setting the `PYICEBERG_MAX_WORKERS` environment variable. The default value depends on the system hardware and Python version. See [the Python documentation](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor) for more details.

## Backward Compatibility

Previous versions of Java (`<1.4.0`) implementations incorrectly assume the optional attribute `current-snapshot-id` to be a required attribute in TableMetadata. This means that if `current-snapshot-id` is missing in the metadata file (e.g. on table creation), the application will throw an exception without being able to load the table. This assumption has been corrected in more recent Iceberg versions. However, it is possible to force PyIceberg to create a table with a metadata file that will be compatible with previous versions. This can be configured by setting the `legacy-current-snapshot-id` property as "True" in the configuration file, or by setting the `PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID` environment variable. Refer to the [PR discussion](https://github.com/apache/iceberg-python/pull/473) for more details on the issue

## Nanoseconds Support

PyIceberg currently only supports upto microsecond precision in its TimestampType. PyArrow timestamp types in 's' and 'ms' will be upcast automatically to 'us' precision timestamps on write. Timestamps in 'ns' precision can also be downcast automatically on write if desired. This can be configured by setting the `downcast-ns-timestamp-to-us-on-write` property as "True" in the configuration file, or by setting the `PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE` environment variable. Refer to the [nanoseconds timestamp proposal document](https://docs.google.com/document/d/1bE1DcEGNzZAMiVJSZ0X1wElKLNkT9kRkk0hDlfkXzvU/edit#heading=h.ibflcctc9i1d) for more details on the long term roadmap for nanoseconds support
