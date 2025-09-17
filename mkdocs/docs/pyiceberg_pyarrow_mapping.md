# PyIceberg (Python types) ⇄ PyArrow Type Mapping

This document lists **PyIceberg Python type classes** and their corresponding **PyArrow** types based on the provided visitor implementation.  
This version uses concrete PyIceberg type names (e.g., `IntegerType`, `TimestampType`) rather than SQL-style tokens (e.g., `INT`, `TIMESTAMP`).

---

## PyIceberg (Python types) → PyArrow

| PyIceberg type class            | PyArrow type                                             |
|---------------------------------|----------------------------------------------------------|
| `BooleanType`                   | `pa.bool_()`                                             |
| `IntegerType`                   | `pa.int32()`                                             |
| `LongType`                      | `pa.int64()`                                             |
| `FloatType`                     | `pa.float32()`                                           |
| `DoubleType`                    | `pa.float64()`                                           |
| `DecimalType(p, s)`             | `pa.decimal128(p, s)`                                    |
| `DateType`                      | `pa.date32()`                                            |
| `TimeType`                      | `pa.time64("us")`                                        |
| `TimestampType`                 | `pa.timestamp("us")`                                     |
| `TimestampNanoType`             | `pa.timestamp("ns")`                                     |
| `TimestamptzType`               | `pa.timestamp("us", tz="UTC")`                           |
| `TimestamptzNanoType`           | `pa.timestamp("ns", tz="UTC")`                           |
| `StringType`                    | `pa.large_string()`                                      |
| `UUIDType`                      | `pa.uuid()`                                              |
| `BinaryType`                    | `pa.large_binary()`                                      |
| `FixedType(L)`                  | `pa.binary(L)`                                           |
| `StructType`                    | `pa.struct([...])` *(fields via `pa.field`)*             |
| `ListType(e)`                   | `pa.large_list(value_type=<element field>)`              |
| `MapType(k, v)`                 | `pa.map_(key_type=<key field>, item_type=<value field>)` |
| `UnknownType`                   | `pa.null()`                                              |

**Field construction**: `pa.field(name, type, nullable=field.optional, metadata={...})`  
**Metadata**: `parquet.field.id` (when `include_field_ids=True`) and `doc` if present.

No other types are supported by the visitor.

---

## PyArrow → PyIceberg (Python types)

| PyArrow type                                  | PyIceberg type class        |
|-----------------------------------------------|-----------------------------|
| `pa.bool_()`                                  | `BooleanType`               |
| `pa.int32()`                                  | `IntegerType`               |
| `pa.int64()`                                  | `LongType`                  |
| `pa.float32()`                                | `FloatType`                 |
| `pa.float64()`                                | `DoubleType`                |
| `pa.decimal128(p, s)`                         | `DecimalType(p, s)`         |
| `pa.date32()`                                 | `DateType`                  |
| `pa.time64("us")`                             | `TimeType`                  |
| `pa.timestamp("us")`                          | `TimestampType`             |
| `pa.timestamp("ns")`                          | `TimestampNanoType`         |
| `pa.timestamp("us", tz="UTC")`                | `TimestamptzType`           |
| `pa.timestamp("ns", tz="UTC")`                | `TimestamptzNanoType`       |
| `pa.large_string()` or `pa.string()`          | `StringType`                |
| `pa.uuid()`                                   | `UUIDType`                  |
| `pa.large_binary()` or variable `pa.binary()` | `BinaryType`                |
| fixed-size `pa.binary(L)`                     | `FixedType(L)`              |
| `pa.struct([...])`                            | `StructType`                |
| `pa.large_list(<element>)` or `pa.list_()`    | `ListType(e)`               |
| `pa.map_(key_type, item_type)`                | `MapType(k, v)`             |
| `pa.null()`                                   | `UnknownType`               |

No other types are supported by the visitor.

---

## Notes and Caveats

- **Strings & binaries:** The visitor emits `large_string` and `large_binary` (64‑bit offsets). `string`/`binary` (32‑bit) still map to `StringType`/`BinaryType` when converting back.
- **Timestamps:** Default precision is microseconds (`"us"`); nano variants are explicit. Zoned timestamps assume UTC.
- **Decimals:** Implemented as `decimal128` in Arrow to avoid partial support for `decimal32/64`.
- **Lists & Maps:** Element/key/value are wrapped as `pa.field`s, preserving nullability (`field.optional`).  
- **Field IDs & docs:** Preserved in Arrow field metadata (`parquet.field.id`, `doc`).

---