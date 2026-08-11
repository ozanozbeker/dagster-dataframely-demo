# dagster-dataframely demo

A Dagster project that puts every UI surface [`dagster-dataframely`](https://github.com/ozanozbeker/dagster-dataframely) touches in front of you at once.
One schema, fourteen definitions across sixteen asset keys, six groups.

Everything here derives from a single `dy.Schema` in `src/dagster_dataframely_demo/schema.py`.
That is the claim the project exists to demonstrate.

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
The asset graph's search bar takes the same selection syntax as `--assets`, so paste each of these in and hit **Materialize**:

1. `* and not group:c_failures and not key:daily_orders` is everything that goes green, in one run.
2. `daily_orders` is partitioned, so this one opens the backfill dialog; take all four days.
3. `group:c_failures` is the three that fail on purpose, so this run ends red.

Ctrl-C ends the session and the run history goes with it.
Tables under `storage/` outlive it, so `rm -rf storage` to start those over too.

## The files

| file | what it is |
| --- | --- |
| `schema.py` | **The one declaration.** Everything else derives from it. |
| `defs/*.py` | One module per asset group, autoloaded by `dg`. This is the code to read. |
| `defs/resources.py` | The two IO managers. |
| `_data.py` | Plumbing. How the fake rows get built. Skip it. |

## What to click

Groups sort alphabetically, so they read in the order below.

| group | asset | what it shows |
| --- | --- | --- |
| `a_catalog` | `raw_orders` | A plain `@dg.asset` over the same rows. The Columns tab it does *not* have is the comparison. |
| | **`orders`** | **Start here.** Columns tab filled in from the schema before the first run, 24 checks behind the blocking gate, four statistics tables and a row sample on the materialization. |
| `b_quarantine` | `defective_raw_orders` | Twenty rows, eight of which break a rule. |
| | `quarantined_orders` | The same rows with `quarantine=dg.AssetOut()`. Seven checks fail at `WARN` and the run stays green. |
| | `quarantined_orders_quarantine` | The rejected rows, one `dy_*` outcome column per rule, plus the `cooccurrence` table. |
| `c_failures` | `strict_orders` | The same rows with no quarantine. `ValidationAbortError`, checks red at `ERROR`, nothing written. |
| | `gated_orders` | `quantity` arrives `Int64`. The blocking `dy_schema__dtypes` check fails and no rule check reports at all. |
| | `doomed_orders` | Every row rejected. `NothingSurvivedError`, quarantine written, good table skipped rather than emptied. |
| `d_granularity` | `orders_by_rule` | `check_granularity="rule"`, the default: 24 checks. |
| | `orders_by_column` | `"column"`: 14, one `dy_col__<column>` per rule-bearing column. |
| | `orders_by_schema` | `"schema"`: 2, and one of those is the gate. |
| `e_storage` | `csv_orders` | `DataframelyCSVIOManager`. Watch the run log name the columns it encoded. |
| | `csv_orders_readback` | The CSV read back. Its green gate check is the proof the codec is an inverse. |
| | `daily_orders` | Four daily partitions, with checks reporting per partition. |
| `f_kit` | `hand_wired_orders` | The same surfaces from `dd.schema_metadata`, `dd.check_specs` and `dd.process`, wired by hand. |

The five rows of the library's failure-policy table are five of these assets: `gated_orders`, `orders`, `strict_orders`, `quarantined_orders`, `doomed_orders`, in that order.

## The three red assets are the point

`strict_orders`, `gated_orders` and `doomed_orders` fail every run, by design.
Each raises a different error from the package, and the message is a surface worth reading.

Keep them out of any bulk materialize, or the aborted run buries the assets you wanted populated.
That is what `not group:c_failures` in the first selection above is for.

## Notes

`daily_orders` is partitioned and cannot share a run with the unpartitioned assets, which is why it gets a step of its own.

Selecting an asset that declares a quarantine needs both of its keys, because the underlying `multi_asset` does not support subsetting: select `group:c_failures` rather than `doomed_orders` on its own.

Tables land in `storage/`, relative to wherever you started `dg dev`.
`DEMO_STORAGE_DIR` overrides it, and the managers take a universal-pathlib path, so `DEMO_STORAGE_DIR=s3://my-bucket/demo` writes to S3 given `s3fs` installed alongside.

### Pointing the demo at a local checkout

`pyproject.toml` depends on the published `dagster-dataframely`, so this project shows the released package.
To watch the UI change as you edit the library, add a source override and re-sync:

```toml
[tool.uv.sources]
dagster-dataframely = { path = "../dagster-dataframely", editable = true }
```

`uv sync --no-sources` goes back to the published wheel without editing the file again.
