import sys, time, polars as pl, pandera.polars as pa
sys.path.insert(0, __file__.rsplit("/",1)[0]); from _common import D
def schema(customer_ids):
    return pa.DataFrameSchema({
        "order_id": pa.Column(str, nullable=False, unique=True),
        "customer_id": pa.Column(str, nullable=False, checks=pa.Check.isin(customer_ids, name="customer_ref")),
        "status": pa.Column(str, checks=pa.Check.isin(["paid","shipped","cancelled","refunded"])),
        "amount": pa.Column(float, checks=pa.Check.ge(0), coerce=True),
    })
for b in ["good","bad"]:
    t=time.perf_counter(); o=pl.read_csv(D/f"orders_{b}.csv"); c=pl.read_csv(D/"customers.csv")
    try:
        schema(c["customer_id"].to_list()).validate(o, lazy=True); print(b,"PASS",f"{time.perf_counter()-t:.2f}s")
    except pa.errors.SchemaErrors as e:
        print(b,"FAIL",f"{time.perf_counter()-t:.2f}s"); print(e.failure_cases)
