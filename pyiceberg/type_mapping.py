"""Type mapping between PyArrow and Iceberg types.

## PyArrow
The Iceberg specification only specifies type mapping for Avro, Parquet, and ORC:

- [Iceberg to Avro](https://iceberg.apache.org/spec/#avro)

- [Iceberg to Parquet](https://iceberg.apache.org/spec/#parquet)

- [Iceberg to ORC](https://iceberg.apache.org/spec/#orc)

Refer to the following tables for type mapping in both direction for PyIceberg types and PyArrow types.

### PyIceberg to PyArrow type mapping

| PyIceberg type class            | PyArrow type                        | Notes                                  |
|---------------------------------|-------------------------------------|----------------------------------------|
| `BooleanType`                   | `pa.bool_()`                        |                                        |
| `IntegerType`                   | `pa.int32()`                        |                                        |
| `LongType`                      | `pa.int64()`                        |                                        |
| `FloatType`                     | `pa.float32()`                      |                                        |
| `DoubleType`                    | `pa.float64()`                      |                                        |
| `DecimalType(p, s)`             | `pa.decimal128(p, s)`               |                                        |
| `DateType`                      | `pa.date32()`                       |                                        |
| `TimeType`                      | `pa.time64("us")`                   |                                        |
| `TimestampType`                 | `pa.timestamp("us")`                |                                        |
| `TimestampNanoType`             | `pa.timestamp("ns")`                |                                        |
| `TimestamptzType`               | `pa.timestamp("us", tz="UTC")`      |                                        |
| `TimestamptzNanoType`           | `pa.timestamp("ns", tz="UTC")`      |                                        |
| `StringType`                    | `pa.large_string()`                 |                                        |
| `UUIDType`                      | `pa.uuid()`                         |                                        |
| `BinaryType`                    | `pa.large_binary()`                 |                                        |
| `FixedType(L)`                  | `pa.binary(L)`                      |                                        |
| `StructType`                    | `pa.struct()`                       |                                        |
| `ListType(e)`                   | `pa.large_list(e)`                  |                                        |
| `MapType(k, v)`                 | `pa.map_(k, v)`                     |                                        |
| `UnknownType`                   | `pa.null()`                         |                                        |

---
### PyArrow to PyIceberg type mapping

| PyArrow type                       | PyIceberg type class        | Notes                          |
|------------------------------------|-----------------------------|--------------------------------|
| `pa.bool_()`                       | `BooleanType`               |                                |
| `pa.int32()`                       | `IntegerType`               |                                |
| `pa.int64()`                       | `LongType`                  |                                |
| `pa.float32()`                     | `FloatType`                 |                                |
| `pa.float64()`                     | `DoubleType`                |                                |
| `pa.decimal128(p, s)`              | `DecimalType(p, s)`         |                                |
| `pa.decimal256(p, s)`              | Unsupported                 |                                |
| `pa.date32()`                      | `DateType`                  |                                |
| `pa.date64()`                      | Unsupported                 |                                |
| `pa.time64("us")`                  | `TimeType`                  |                                |
| `pa.timestamp("us")`               | `TimestampType`             |                                |
| `pa.timestamp("ns")`               | `TimestampNanoType`         |                                |
| `pa.timestamp("us", tz="UTC")`     | `TimestamptzType`           |                                |
| `pa.timestamp("ns", tz="UTC")`     | `TimestamptzNanoType`       |                                |
| `pa.string()` / `pa.large_string()`| `StringType`                |                                |
| `pa.uuid()`                        | `UUIDType`                  |                                |
| `pa.binary()` / `pa.large_binary()`| `BinaryType`                |                                |
| `pa.binary(L)`                     | `FixedType(L)`              | Fixed-length byte arrays       |
| `pa.struct([...])`                 | `StructType`                |                                |
| `pa.list_(e)` / `pa.large_list(e)` | `ListType(e)`               |                                |
| `pa.map_(k, v)`                    | `MapType(k, v)`             |                                |
| `pa.null()`                        | `UnknownType`               |                                |

---

### Notes
- PyIceberg `GeometryType` and `GeographyType` types are mapped to a GeoArrow WKB extension type.
Otherwise, falls back to `pa.large_binary()` which stores WKB bytes.
"""
