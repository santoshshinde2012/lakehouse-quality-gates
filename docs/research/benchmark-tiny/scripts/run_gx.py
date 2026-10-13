import sys, time, great_expectations as gx, great_expectations.expectations as gxe
sys.path.insert(0, __file__.rsplit("/",1)[0]); from _common import con
ctx = gx.get_context(mode="ephemeral")
batch_def = ctx.data_sources.add_pandas("duck").add_dataframe_asset("orders").add_batch_definition_whole_dataframe("all")
for b in ["good","bad"]:
    t=time.perf_counter(); c=con(b)
    df=c.sql("select * from orders").df(); ids=[r[0] for r in c.sql("select customer_id from customers").fetchall()]
    suite = gx.ExpectationSuite(name=f"silver_orders_{b}")
    for e in [gxe.ExpectColumnValuesToNotBeNull(column="order_id"), gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
              gxe.ExpectColumnValuesToBeUnique(column="order_id"),
              gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["paid","shipped","cancelled","refunded"]),
              gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0),
              gxe.ExpectColumnValuesToBeInSet(column="customer_id", value_set=ids)]:
        suite.add_expectation(e)
    r = batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite, result_format="COMPLETE")
    print(b, "success=", r.success, f"{time.perf_counter()-t:.2f}s")
    for x in r.results:
        if not x.success: print("  FAIL", x.expectation_config.type, x.expectation_config.kwargs.get("column"), "unexpected=", x.result.get("partial_unexpected_list"), "idx=", x.result.get("unexpected_index_list"))
