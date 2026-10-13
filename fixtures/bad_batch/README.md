# bad_batch fixture

The repo's retail sample (`customers.csv`, `orders_day1.csv`, `orders_day2.csv`) plus injected defects.
Each defect is caught by a named contract check:

| Defect | File / row | Caught by |
|---|---|---|
| Negative amount `-45.00` | orders_day3 `o-2001` | bronze `Amount is not negative` (warn) → silver `Amount is not negative` (fail) |
| Orphan customer `c-99` | orders_day3 `o-2002` | silver `Customer exists in silver.customers` (fail) |
| Outlier amount `25000` | orders_day3 `o-2003` | silver `Amount below the outlier ceiling` (warn), gold `Average order value is plausible` (warn) |
| Missing customer_id | orders_day3 `o-2004` | bronze `Every order has a customer` (fail) |
| Unknown status `teleported` | orders_day3 `o-2005` | bronze status check (warn); silver drops the row |
| Same order twice in one file | orders_day3 `o-2005` | bronze `No order duplicated inside one source file` (fail) |
| Order dated 2099 | orders_day3 `o-2006` | silver `No order timestamped in the future` (fail), gold `No metrics for future days` (fail) |
| Malformed email | customers `c-11` | silver `Email is well-formed` (fail) |
| Duplicate email | customers `c-12` | silver `Email unique` (fail) |
