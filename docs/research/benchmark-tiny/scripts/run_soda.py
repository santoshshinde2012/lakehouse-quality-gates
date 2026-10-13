import sys, time
from soda_core.contracts import verify_contract_locally
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl
sys.path.insert(0, __file__.rsplit("/",1)[0]); from _common import con
CFG = __file__.rsplit("/",2)[0] + "/configs/soda_orders.yml"
for b in ["good","bad"]:
    t=time.perf_counter(); c=con(b)
    ds = DuckDBDataSourceImpl.from_existing_cursor(c, name="duck")
    r = verify_contract_locally(data_sources=[ds], contract_file_path=CFG)
    print(b, "ok=", r.is_ok, f"{time.perf_counter()-t:.2f}s")
    for cr in r.contract_verification_results:
        for ch in cr.check_results:
            print("  ", ch.outcome.name, ch.check.type, getattr(ch.check,'column_name',None), getattr(ch,'diagnostic_metric_values',None))
