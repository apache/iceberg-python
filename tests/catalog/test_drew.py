import pyarrow as pa

from pyiceberg.catalog.glue import GlueCatalog

# pd.set_option('display.max_rows', None)


def test():
    catalog = GlueCatalog("drew")
    table_name = "demo1.vec"
    #
    # catalog = load_catalog(
    #   "catalog_name",
    #   **{
    #     "type": "rest",
    #     "warehouse":"arn:aws:s3tables:us-east-1:580974493829:bucket/drew",
    #     "uri": "https://s3tables.us-east-1.amazonaws.com/iceberg",
    #     "rest.sigv4-enabled": "true",
    #     "rest.signing-name": "s3tables",
    #     "rest.signing-region": "us-east-1"
    #   }
    # )

    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
            {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
            {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
            {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
            {"city": "Paris", "lat": 48.864716, "long": 2.349014},
        ],
    )

    # tbl = catalog.create_table("demo1.cities2", schema=df.schema, location="s3://dru-iad/demo1/cities2")
    table = catalog.load_table(("yeehaw", "lmfao"))
    print(table)
