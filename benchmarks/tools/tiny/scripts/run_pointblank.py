import sys, time, polars as pl, pointblank as pb
sys.path.insert(0, __file__.rsplit("/",1)[0]); from _common import D
def run(batch):
    o = pl.read_csv(D/f"orders_{batch}.csv"); c = pl.read_csv(D/"customers.csv")
    v = (pb.Validate(data=o, tbl_name=f"silver.orders[{batch}]", thresholds=pb.Thresholds(warning=1, error=1))
         .col_vals_not_null(columns="order_id")
         .col_vals_not_null(columns="customer_id")
         .rows_distinct(columns_subset=["order_id"])
         .col_vals_in_set(columns="status", set=["paid","shipped","cancelled","refunded"])
         .col_vals_ge(columns="amount", value=0)
         .col_vals_in_set(columns="customer_id", set=c["customer_id"].to_list())
         .interrogate())
    return v
for b in ["good","bad"]:
    t=time.perf_counter(); v=run(b); dt=time.perf_counter()-t
    print(b, "all_passed=", v.all_passed(), f"{dt:.2f}s", "failed_steps=", [i for i,p in v.n_failed().items() if p])
    if b=="bad":
        for k,df in v.get_data_extracts().items(): print("step",k, df.to_dicts() if hasattr(df,'to_dicts') else df)
