import pyarrow as pa

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    BooleanType,
    IntegerType,
    LongType
)
def test():
    catalog_name = "rutuja"

    test_catalog = GlueCatalog(catalog_name, warehouse="s3://rubabar/test")

    namespace_name = "test"
    schema = Schema(
        NestedField(field_id=1, name="boolean", field_type=BooleanType(), required=True),
        NestedField(field_id=2, name="integer", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="long", field_type=LongType(), required=True),
    )
    table_iden = (namespace_name, "table1")
    # test_catalog.create_namespace(namespace_name)
    # table = test_catalog.create_table(table_iden, schema)
    table = test_catalog.load_table(table_iden)
    test_data = pa.Table.from_pydict(
        {
            "boolean": [True],
            "integer": [125],
            "long": [42327],
        },
        schema=pa.schema(
            [
                pa.field("boolean", pa.bool_(), nullable=False),
                pa.field("integer", pa.int32(), nullable=False),
                pa.field("long", pa.int64(), nullable=False),
            ]
        ),
    )
    table.append(test_data)

    test_data1 = pa.Table.from_pydict(
        {
            "boolean": [False],
            "integer": [188],
            "long": [234321],
        },
        schema=pa.schema(
            [
                pa.field("boolean", pa.bool_(), nullable=False),
                pa.field("integer", pa.int32(), nullable=False),
                pa.field("long", pa.int64(), nullable=False),
            ]
        ),
    )
    table.append(test_data1)
    # table.delete("boolean == False")
if __name__ == "__main__":
    test()