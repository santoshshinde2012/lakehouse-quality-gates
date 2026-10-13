# Contracts guide

> [!NOTE]
> **At a glance.** One Soda Core v4 YAML file per dataset under `contracts/<pipeline>/<layer>/`. Five contracts
> hold 46 checks: 14 in the bronze gate, 21 in silver, 11 in gold.

Location: `contracts/<pipeline>/<layer>/<table>.yml`. These are Soda Core v4 contract-language files
([reference](https://docs.soda.io/reference/contract-language-reference)). The only addition is a
`{{ dataset_root }}` token, rendered per engine.

## Example

An excerpt of [`contracts/retail/silver/orders.yml`](../../contracts/retail/silver/orders.yml):

```yaml
dataset: "{{ dataset_root }}/silver/orders"

checks:
  - schema:
      name: Silver orders schema is unchanged
  - failed_rows:
      name: No order timestamped in the future
      expression: order_ts > CURRENT_TIMESTAMP

columns:
  - name: customer_id
    data_type: varchar
    checks:
      - missing:
          name: Customer id present
      - invalid:
          name: Customer exists in silver.customers
          valid_reference_data:
            dataset: "{{ dataset_root }}/silver/customers"
            column: customer_id
```

## Conventions

- Every check has a `name` written as a plain-language business rule. That name shows up in reports,
  Slack and the gate reasons.
- `attributes: {layer, pipeline}` sit at contract level, so `soda contract verify -cf attributes.layer=...`
  also works.
- Every column is listed with a `data_type`, and the `schema` check is strict (no extra columns, same
  order). Schema drift therefore always fails, by design.
- Warn-only checks use `threshold: {must_be: 0, level: warn}`.
- When the same check type appears twice on one column, the second carries a `qualifier`, because Soda
  needs a unique identity for each check.
- Tunables are Soda variables with defaults (`variables:`). Override them with `--set KEY=VALUE`. The CLI
  turns numeric-looking values into numbers, because Soda 4.26 rejects strings for threshold keys.
- SQL inside `failed_rows` and `metric` sticks to ANSI SQL that both DuckDB and Trino accept
  (`CURRENT_TIMESTAMP`, `CURRENT_DATE`, unqualified `schema.table`).

## Checks per contract

| Contract | Checks |
|---|---|
| `bronze/orders_raw` (10) | schema · row_count ≥ 1 · freshness `_ingested_at` < `BRONZE_MAX_AGE_HOURS` (24) · duplicate (`order_id`, `_source_file`) · missing `order_id`, `customer_id`, `order_ts`, `amount` · invalid status `LOWER(TRIM(status))` ∈ {paid, shipped, cancelled, refunded, pending} (warn) · invalid amount < 0 (warn) |
| `bronze/customers_raw` (4) | schema · row_count · missing `customer_id` · missing email (warn) |
| `silver/customers` (9) | schema · row_count · missing and duplicate `customer_id` · missing name · missing, invalid regex and duplicate email · missing `signup_date` |
| `silver/orders` (12) | schema · row_count · failed_rows `order_ts > CURRENT_TIMESTAMP` · missing and duplicate `order_id` · missing `customer_id` + `valid_reference_data` → `silver/customers` · missing `order_ts` · status ∈ canonical set · missing amount, `valid_min: 0`, `valid_max: MAX_ORDER_AMOUNT` (warn) |
| `gold/daily_order_metrics` (11) | schema · row_count · failed_rows `cancelled + refunded > orders` · failed_rows `order_date > CURRENT_DATE` · metric: \|SUM(gold.orders) − COUNT(silver.orders)\| = 0 · missing and duplicate `order_date` · `orders ≥ 1` · revenue ≥ 0 · AOV ≥ 0 · AOV ≤ `MAX_AVG_ORDER_VALUE` (warn) |

Totals per gate: bronze 14, silver 21, gold 11 (46).

Why bronze **warns** where silver **fails**: [use case](../overview/use-case.md#solution).

## Adding a contract

1. Create `contracts/retail/<layer>/<table>.yml` with `dataset: "{{ dataset_root }}/<layer>/<table>"`.
2. `make validate`: Soda parses it and reports typos such as `Invalid check type 'missingg'`.
3. Add a defect for it to `fixtures/bad_batch` and assert on it in `tests/integration/test_soda_duckdb.py`.
4. `make check demo`.

## Check types available in the free Soda Core 4.26

`schema, missing, invalid, duplicate, row_count, aggregate, metric, freshness, failed_rows` (as listed by
Soda's own validator). `reconciliation` and `group_by` need a licence. The silver↔gold reconciliation
here is therefore written as a `metric` query.
