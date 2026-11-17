# dbt Incremental Models and Hooks

Study notes with examples for dbt on AWS Glue (Spark, Hudi/Iceberg) with Athena.
Check your adapter version's docs before relying on any adapter-specific detail.

## Contents

1. [Incremental models](#1-incremental-models)
2. [Incremental strategies](#2-incremental-strategies)
3. [Incremental pitfalls](#3-incremental-pitfalls)
4. [Hooks](#4-hooks)
5. [Hook pitfalls](#5-hook-pitfalls)
6. [Practice plan](#6-practice-plan)

---

## 1. Incremental models

A normal model (`materialized='table'`) rebuilds the whole table on every run.
An incremental model builds the full table once, then on later runs processes
only new or changed rows and writes them into the existing table.

| Run | What dbt does |
|---|---|
| First run (table does not exist) | Runs the full query and creates the table |
| Later runs | Runs the query with your filter, then inserts or merges only those rows |
| `--full-refresh` | Drops and rebuilds from scratch |

### Key pieces

- `is_incremental()` is true only when the table already exists, the model is
  incremental, and you are not using `--full-refresh`. Wrap your filter in it so
  the first run still loads everything.
- `{{ this }}` refers to the model's own existing table. Use it to find the
  latest row already loaded.
- `unique_key` identifies a row so dbt can update it instead of duplicating it.

### Sample model

```sql
-- models/staging/stg_orders_incremental.sql
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    file_format='hudi'
  )
}}

select
    order_id,
    customer_id,
    amount,
    _loaded_at
from {{ source('raw_source_freshness', 'orders') }}

{% if is_incremental() %}
  where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
```

What happens:

1. Run 1: all rows load because `is_incremental()` is false.
2. New data arrives: order 4 is added and order 3 is corrected.
3. Run 2: only rows newer than the current max `_loaded_at` are selected.
   Order 4 is inserted and order 3 is updated in place via `unique_key`.

---

## 2. Incremental strategies

`incremental_strategy` decides how the new batch is written into the existing
table. Your `is_incremental()` filter decides which rows are selected.

Supported strategies depend on the dbt-glue version. This project uses
dbt-glue 1.12.4, whose incremental macros generate `MERGE INTO` for Iceberg
and support `append`, `insert_overwrite` and `merge`. Always verify adapter
support and set `incremental_strategy` explicitly.

### Running example

Existing table:

| order_id | amount | order_date |
|---|---|---|
| 1 | 250 | 2026-11-14 |
| 2 | 120 | 2026-11-15 |
| 3 | 80 | 2026-11-16 |

New batch: order 3 corrected to `95`, plus new order 4 (`60`, 2026-11-16).

### append

Plain `insert into` with the new rows.

- Result: orders 1, 2, 3 (80), 3 (95), 4. Order 3 is duplicated.
- Use for: immutable data such as logs and events.
- Watch out: duplicates, and reruns of the same batch double-load.
- Works with any file format.

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}
```

### merge

Atomic `MERGE`: matches on `unique_key`, updates matches, inserts the rest.

- Result: orders 1, 2, 3 (95), 4. No duplicates.
- Use for: mutable data such as orders, customers and CDC feeds.
- Requires a table format with row-level updates. This project's installed
  dbt-glue adapter supports Iceberg `MERGE INTO`; other adapter versions may
  differ. Plain Parquet does not support row-level merge.
- Without a `unique_key` it behaves like `append`.
- The key must be unique in the incoming batch. Deduplicate with
  `row_number()` first if needed.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    file_format='iceberg'
) }}
```

### insert_overwrite

Works on partitions, not rows. With `partition_by`, every partition present in
the query result is replaced. Without `partition_by`, the whole table is
replaced.

- Using `partition_by=['order_date']`: the 2026-11-16 partition is replaced by
  the batch, so order 3 (95) and order 4 remain and old order 3 (80) is gone.
  Other partitions are untouched.
- Use for: time-partitioned facts you can recompute cheaply, no unique key needed.
- Watch out: re-select all rows for each partition you touch, or the rest of
  that partition is deleted.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['order_date']
) }}

select order_id, amount, order_date
from {{ source('raw_source_freshness', 'orders') }}
{% if is_incremental() %}
  where order_date >= date_add(current_date(), -1)
{% endif %}
```

### Other strategies (not in the dbt-glue docs I checked)

- `delete+insert`: delete rows whose key appears in the batch, then insert.
  Common on Snowflake, Redshift and Postgres.
- `microbatch` (dbt 1.9+): splits the run into time-based batches using
  `event_time`, which makes backfills and retries easy. Adapter support varies.
- `replace_where`: Databricks/Delta strategy that replaces rows matching a
  predicate.

### Comparison

| Strategy | Updates existing rows | Needs `unique_key` | Special format | Best for |
|---|---|---|---|---|
| append | No | No | No | Immutable events |
| merge | Yes | Yes | Hudi/Iceberg on Glue | Mutable records, CDC |
| insert_overwrite | Replaces partitions | No | Partitioned table | Daily partitioned facts |
| delete+insert | Yes | Yes | No | Warehouses without merge |
| microbatch | Replaces batches | No (`event_time`) | Adapter-dependent | Large time-series, backfills |

---

## 3. Incremental pitfalls

1. Late-arriving data. A `max(_loaded_at)` filter permanently skips rows that
   arrive late with older timestamps. Use a lookback window and let
   `unique_key` deduplicate:

   ```sql
   {% if is_incremental() %}
     where _loaded_at > (select max(_loaded_at) from {{ this }}) - interval 3 days
   {% endif %}
   ```

2. Wrong watermark column. Use an ingestion timestamp (`_loaded_at`), not a
   business date, or corrections to old records are missed.
3. Hard deletes in the source are never noticed. Use periodic `--full-refresh`
   or a soft-delete flag.
4. Schema changes. New columns break the next run unless `on_schema_change` is
   `append_new_columns` or `sync_all_columns`. The default `ignore` silently
   drops them.
5. Logic drift. After changing transformation logic, old rows keep the old
   logic until you run `dbt run --full-refresh --select <model>`.
6. Missing `unique_key` with merge causes silent duplicates.

---

## 4. Hooks

A hook is SQL or a macro call that dbt runs automatically at a specific point
in a run.

| Hook | Runs | Scope | Defined in |
|---|---|---|---|
| `pre-hook` | Before a model, seed or snapshot builds | One resource | Model config or `dbt_project.yml` |
| `post-hook` | After a model, seed or snapshot builds | One resource | Model config or `dbt_project.yml` |
| `on-run-start` | Once at the start of the invocation | Whole run | `dbt_project.yml` |
| `on-run-end` | Once at the end of the invocation | Whole run | `dbt_project.yml` |

Order for one `dbt run`:

```
on-run-start
  -> pre-hook(model A) -> build model A -> post-hook(model A)
  -> pre-hook(model B) -> build model B -> post-hook(model B)
on-run-end
```

### pre-hook and post-hook

```sql
-- models/marts/fct_orders.sql
{{ config(
    materialized='table',
    pre_hook="insert into audit.run_log values ('fct_orders', 'started', current_timestamp())",
    post_hook=[
      "insert into audit.run_log values ('fct_orders', 'finished', current_timestamp())",
      "{{ log_row_count(this) }}"
    ]
) }}

select * from {{ ref('stg_orders') }}
```

- Use a list for multiple statements. They run in order.
- In `config()` use `pre_hook` / `post_hook`. In `dbt_project.yml` you can use
  `+pre-hook` / `+post-hook`.
- Apply to a whole folder:

```yaml
models:
  my_project:
    marts:
      +post-hook:
        - "insert into audit.model_builds values ('{{ this.name }}', current_timestamp())"
```

### on-run-start and on-run-end

```yaml
# dbt_project.yml
on-run-start:
  - "create table if not exists audit.run_log (model string, status string, ts timestamp)"
  - "{{ log('Starting run in target: ' ~ target.name, info=True) }}"

on-run-end:
  - "{{ log_run_results(results) }}"
```

`on-run-end` also exposes:

- `results`: one result object per model, test or seed (`.node.name`,
  `.status`, `.execution_time`).
- `schemas` and `database_schemas`: where dbt built objects in this run.

```sql
-- macros/log_run_results.sql
{% macro log_run_results(results) %}
  {% if execute %}
    {% for res in results %}
      {% set q %}
        insert into audit.run_log
        values ('{{ res.node.name }}', '{{ res.status }}', current_timestamp())
      {% endset %}
      {% do run_query(q) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
```

The `{% if execute %}` guard stops the macro from running during the parse phase.

### Common uses

- Audit and logging of model runs, status and timing.
- Table maintenance after large builds (`OPTIMIZE`, `VACUUM`, `ANALYZE`);
  syntax depends on your engine and table format.
- Setup in `on-run-start`: schemas, audit tables, UDFs.
- Permissions on warehouses that use SQL grants.
- Notifications from `on-run-end`, though an orchestrator usually fits better.

### Hooks vs run-operation

`dbt run-operation my_macro` runs a macro on demand as its own command. Hooks
run automatically as part of another command. Use `run-operation` for one-off
tasks and hooks for things that must happen on every run.

---

## 5. Hook pitfalls

1. A failing pre-hook stops the model from building. A failing post-hook marks
   the model as failed even though the table was created.
2. On warehouses with transactions (Postgres, Redshift), some statements such
   as `vacuum` cannot run in a transaction. Use `before_begin(...)` and
   `after_commit(...)`. This usually does not apply to Spark-based adapters.
3. On AWS, access is often managed through Lake Formation or IAM rather than
   SQL `GRANT`, so permission hooks may not apply. Check the dbt-glue docs.
4. Do not put heavy business logic in hooks. They are invisible in the DAG. If
   something must be a dependency, make it a model.
5. Hooks on incremental models run every time, so make them idempotent.
6. Verify in your dbt version which commands trigger hooks instead of assuming.

---

## Implemented project pattern

`models/marts/facts/fact_sales.sql` uses an Iceberg incremental `merge` with
`order_id` and `product_id` as its composite key. The query uses `{{ this }}`
to find the current maximum `order_date` and reprocesses a configurable
seven-day lookback (`fact_sales_lookback_days`). This makes reruns idempotent
and lets recent corrections be merged into existing rows. The
`dbt_utils.unique_combination_of_columns` tests on `int_sales` and `fact_sales`
enforce that the key is unique in both the incoming data and target.

The raw sources currently expose no reliable ingestion/update timestamp. A
business-date lookback cannot detect changes older than its window; use
`dbt run --full-refresh --select +fact_sales` to rebuild the fact and its dbt
parents for those corrections, or add a source ingestion timestamp/change-data
feed before relying on a strict watermark.

The project has `on-run-start` and `on-run-end` logging hooks. The fact model's
post-hook runs `ANALYZE TABLE {{ this }} COMPUTE STATISTICS` so Spark can use
table statistics for query planning. Since hooks run against Glue, confirm
permissions and Spark support in the active runtime before deployment.

An idempotent model pre-hook creates each Iceberg namespace with
`CREATE NAMESPACE IF NOT EXISTS glue_catalog.<schema>`. This is needed because
the dbt-glue schema setup can fail to create the Glue database for a
catalog-qualified schema; the pre-hook makes the required namespace explicit
before the model writes its table.

## 6. Practice plan

Incremental:

1. Build the same model with `append`, `merge` and `insert_overwrite`.
2. Run each with the same two-step data: initial load, then the corrected-order
   batch from section 2. Compare the results side by side.
3. Add the 3-day lookback and test with a late-arriving row.
4. Check source freshness first, then build incrementally.

Hooks:

1. Add an `on-run-start` hook that creates `audit.run_log`.
2. Add `log_run_results` to `on-run-end` and check every model gets a row.
3. Add a model-level `post_hook` that stores the row count of `{{ this }}`.
4. Break a post-hook on purpose and see how dbt reports the model.
5. Add two hooks with `log()` messages and read the ordering in the run output.


-----------------------------------------
What {{ this }} is

{{ this }} is a dbt Jinja variable that refers to the model currently being built, meaning its own target table in your warehouse. It resolves to the fully qualified name, such as analytics_db.analytics.stg_orders_incremental.

It’s a relation object, not just a string. You can use it directly in SQL, and it also has parts you can read:

Expression	Gives you
{{ this }}	database.schema.table (quoted per adapter rules)
{{ this.database }}	the database
{{ this.schema }}	the schema
{{ this.name }} or {{ this.identifier }}	the table name

On Glue and Athena, database and schema are often the same thing, and the exact rendering depends on your adapter.
