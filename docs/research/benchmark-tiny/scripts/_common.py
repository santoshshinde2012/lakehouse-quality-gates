"""Shared fixtures: the same 6 checks for every tool on silver.orders.
Expected on bad batch: missing customer_id(1 row O3), duplicate order_id(2 rows O1),
invalid status(1 row O5), negative amount(1 row O6), orphan customer(1 row O4)."""
import pathlib, duckdb
D = pathlib.Path(__file__).resolve().parent.parent / "data"
EXPECTED_BAD_FAILS = {"order_id_missing": False, "customer_id_missing": True, "order_id_unique": True,
                      "status_valid": True, "amount_min": True, "customer_ref": True}
def con(batch):
    c = duckdb.connect()
    c.execute(f"create table orders as select * from read_csv_auto('{D}/orders_{batch}.csv', all_varchar=false)")
    c.execute(f"create table customers as select * from read_csv_auto('{D}/customers.csv')")
    return c
