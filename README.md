# dagster-dataframely demo

A Dagster project that puts every UI surface [`dagster-dataframely`](https://github.com/ozanozbeker/dagster-dataframely) touches in front of you at once.
One schema, twenty-four definitions across twenty-nine asset keys, twelve groups.

Everything here derives from a single `dy.Schema` in `src/dagster_dataframely_demo/schema.py`.
That is the claim the project exists to demonstrate.

Four tables in `base` hold the rows; every other asset reads one of them and has exactly one parent.
So each group is a chain you can follow rather than a fan off a shared root, and the lineage view stays legible on a projector.

## Getting started

```bash
uv sync
uv run dg dev
```

Then open <http://localhost:3000>.

There is no `DAGSTER_HOME` to set.
`dg dev` builds its instance in a temp directory and deletes it on exit, so every session starts empty and leaves nothing behind.

That is also why the runs below happen in the UI rather than the CLI.
A `dg launch` in another shell gets a throwaway instance of its own, and nothing it records reaches the tab you have open.

Materialize before you look.
Half these surfaces only exist once an asset has run.
The asset graph's search bar takes the same selection syntax, so paste each of these in and hit **Materialize**:

1. `* and not group:"failure/*" and not group:partitions` is everything that goes green, in one run.
2. `daily_orders` opens the backfill dialog: take all five days.
3. `regional_orders or regional_orders_quarantine` opens it too: take all ten cells.
4. `orders_rollup` fans in over the five days, so run it after step 2.
5. `group:"failure/*"` is all four exits at once.
   Three of them fail on purpose, so this run ends red.

A quoted glob is how you select a whole subtree: `group:"failure/*"` takes all four subgroups, `group:failure/shape` takes one.

Ctrl-C ends the session and the run history goes with it.
Tables under `storage/` outlive it, so `rm -rf storage` to start those over too.

## The files

| file | what it is |
| --- | --- |
| `schema.py` | **The one declaration.** Everything else derives from it. |
| `defs/base.py` | The four tables everything else reads. Plain `@dg.asset`s, no schema. |
| `defs/*.py` | One module per subject, autoloaded by `dg`. This is the code to read. |
| `defs/resources.py` | The two IO managers. |
| `_data.py` | Plumbing. How the fake rows get built. Skip it. |

## What to click

Groups sort alphabetically, so they read in the order below.
Each one starts from a `base` table and runs in a straight line, so you can follow a group in the lineage view without tracing which upstream fed which asset.

| group | asset | what it shows |
| --- | --- | --- |
| `base` | `raw_orders` | Twelve clean lines, declaring nothing about their own shape. The Columns tab it does *not* have is the comparison for everything below. |
| | `defective_raw_orders` | The same twelve plus eight that break a rule. |
| | `hopeless_raw_orders` | Three lines, all invalid, so nothing can survive a filter. |
| | `mistyped_raw_orders` | The clean lines with `quantity` widened to `Int64`. The dtype drifted upstream, which is where dtype drift comes from. |
| `catalog` | **`orders`** | **Start here.** Columns tab filled in from the schema before the first run, 24 checks behind the blocking shape check, four statistics tables and a row sample on the materialization. |
| | `orders_undescribed` | The same asset with no `description=`, so the catalog shows the schema's docstring instead of the function's. |
| `failure/no_quarantine` | `strict_orders` | Rows rejected with nowhere to route them. `ValidationAbortError`, checks red at `ERROR`, nothing written. |
| `failure/nothing_survives` | `doomed_orders` | Every row rejected. `NothingSurvivedError`, quarantine written, valid table skipped rather than emptied. |
| `failure/quarantine` | `quarantined_orders` | The same rows as `strict_orders` with `quarantine=dg.AssetOut()`. Seven checks fail at `WARN` and the run stays green. |
| | `quarantined_orders_quarantine` | The invalid rows, one `dy_*` outcome column per rule, plus the `cooccurrence` table. Its only parent is `quarantined_orders`, not the base table both came from. |
| `failure/shape` | `mistyped_orders` | `quantity` arrives `Int64`. The blocking `dy_schema__dtypes` check fails and no rule check reports at all. |
| `granularity` | `orders_by_rule` | `check_granularity="rule"`, the default: 24 checks. |
| | `orders_by_column` | `"column"`: 14, one `dy_col__<column>` per rule-bearing column. |
| | `orders_by_schema` | `"schema"`: 2, and one of those is the shape check. |
| `metadata` | `annotated_orders` | A returned `dg.MaterializeResult`: your metadata, tags and data version on the event. |
| | `context_annotated_orders` | `context.add_asset_metadata` with `asset_key=`, and the collision it wins that a returned result loses. |
| `partitions` | `daily_orders` | Five daily partitions, each slicing its own day out of the whole of `raw_orders`. Checks report per partition. |
| | `regional_orders` | Day crossed with region: ten cells, nested one directory per dimension on disk. |
| | `orders_rollup` | The fan-in, `dict[str, pl.LazyFrame]` keyed by partition. |
| `storage/csv` | `csv_orders` | `DataframelyCSVIOManager`. Watch the run log name the columns it encoded. |
| | `csv_orders_readback` | The CSV read back. Its green shape check is the proof the codec is an inverse. |
| `storage/lazy` | `streamed_extract` | A plain `@dg.asset` returning a `pl.LazyFrame`, so the plan sinks straight to storage. |
| | `validated_stream` | The same lazy return under a schema: staged, read back, validated. The computation streams either way. |
| `wiring` | `hand_wired_orders` | The same surfaces from `dd.wiring`, assembled by hand, quarantine included. |
| | `hand_wired_clean_orders` | The same again with one out, which is all a plain `@dg.asset` needs. |

`failure/*` is the library's failure-policy table, four rows of it.
The fifth is `catalog/orders`, where every row is valid and the question never comes up.

## Screenshots

The library's README has no images yet, and this project exists to supply them.
Shoot in this order: each one needs the run above it to have happened.

| # | asset | what to capture |
| --- | --- | --- |
| 1 | `quarantined_orders` | **Hero.** The asset page with the checks panel open, one check red at `WARN` while the run stayed green. |
| 2 | `orders` | The Columns tab: dtypes, descriptions, `tracking_id` unique, the composite primary key at table level, the constraints beside each column, and `amount`'s column tags. |
| 3 | `orders_undescribed` | The overview panel, showing the schema's docstring as the description. |
| 4 | `orders` | The check list at `rule` granularity, with descriptions: `paid_orders_have_amount` from its docstring, `amount >= 0.00` rendered, `line_numbers_are_dense` falling back to its own name. |
| 5 | `orders_by_rule`, `orders_by_column`, `orders_by_schema` | Three crops of the check lists side by side: 24, 14, 2. |
| 6 | `quarantined_orders` | One red check expanded, showing `dy_failed_sample` with the rows it rejected. |
| 7 | `quarantined_orders` and `strict_orders` | The same rule red at `WARN` and at `ERROR`. The severity is the failure policy, visible. |
| 8 | lineage | `defective_raw_orders` into `quarantined_orders` into its quarantine, one edge each rather than the quarantine hanging off the raw table too. Then `orders`, where the sibling is absent. |
| 9 | `orders` | The materialization: `dagster/row_count`, `sample`, the four `stats/*` tables, and the manager's `path`, `bytes_written`, `dagster/storage_kind`. |
| 10 | `quarantined_orders_quarantine` | The `cooccurrence` table, where `ORD-0015` reads as one row that tripped three rules. |
| 11 | `daily_orders`, then `regional_orders` | The partition grid, one dimension and then two. |
| 12 | `annotated_orders` | The materialization carrying `source`, `extract/*`, the user-provided data version tag, and `dagster/row_count` at 12 rather than the 999 the transform returned. |

Two more worth having if the library's docs grow a hand-wiring page: `hand_wired_orders`' Columns tab beside `orders`' (identical, which is the point), and `context_annotated_orders` showing `dagster/row_count` at 999, which is the argument for preferring the returned result.

Shoot light or dark consistently, crop tight, and note the Dagster version wherever the images land: they are assertions about someone else's UI.
The rows in `sample` and `dy_failed_sample` are the fake ones from `_data.py`, which is the only reason they are safe to publish.

## The red assets are the point

`strict_orders`, `mistyped_orders` and `doomed_orders` fail every run, by design, and `doomed_orders_quarantine` is written by the run that fails.
Each raises a different error from the package, and the message is a surface worth reading.

Keep them out of any bulk materialize, or the aborted run buries the assets you wanted populated.
That is what `not group:"failure/*"` in the first selection is for.

It takes `quarantined_orders` out with them, which is why step 5 runs the whole subtree: four exits in one run, three red and one green, which is the comparison the group exists for.

## Notes

Partitioned assets cannot share a run with unpartitioned ones, which is why `partitions` gets steps of its own.

Selecting an asset that declares a quarantine needs both of its keys, because the underlying `multi_asset` does not support subsetting: select `group:failure/nothing_survives` rather than `doomed_orders` on its own, and `regional_orders or regional_orders_quarantine` rather than either alone.

Tables land in `storage/`, relative to wherever you started `dg dev`.
`DEMO_STORAGE_DIR` overrides it, and the managers take a universal-pathlib path, so `DEMO_STORAGE_DIR=s3://my-bucket/demo` writes to S3 given `s3fs` installed alongside.

The partitioned assets hold disjoint orders rather than the same rows restamped, so `orders_rollup` can concatenate all five days and still satisfy the primary key.

### Pointing the demo at a local checkout

`pyproject.toml` depends on the published `dagster-dataframely`, so this project shows the released package.
To watch the UI change as you edit the library, add a source override and re-sync:

```toml
[tool.uv.sources]
dagster-dataframely = { path = "../dagster-dataframely", editable = true }
```

`uv sync --no-sources` goes back to the published wheel without editing the file again.
