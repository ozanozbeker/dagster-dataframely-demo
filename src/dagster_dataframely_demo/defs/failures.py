"""Group `c_failures`: the three ways a run stops, each in its own asset.

**These fail on every run, deliberately.** A red gate check and an aborted run are surfaces too, and each of the three raises a different error from the package with a different thing to say.

Keep them out of a bulk materialize, or the aborted run buries the assets you wanted populated.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "c_failures"


@dd.dataframely_asset(schema=Orders, group_name=GROUP)
def strict_orders(defective_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The same twenty lines with no quarantine declared, so the run fails and writes nothing.

    Without a quarantine every row has to be good. The checks fail at `ERROR`, the run raises `ValidationAbortError`, and the last-known-good table stays in place. Landing the survivors and dropping the rest is the failure this package exists to make visible, so it is not reachable by configuration.
    """
    return defective_raw_orders


@dd.dataframely_asset(schema=Orders, group_name=GROUP)
def gated_orders() -> pl.DataFrame:
    """Clean rows arriving with `quantity` as `Int64` where the schema declares `Int32`.

    The blocking `dy_schema__dtypes` check fails and the run raises `SchemaGateError` before a single row is filtered, so no rule check reports at all. The package never casts: silently widening a dtype is how a thousandfold error reaches a table nobody re-reads.
    """
    return _data.mistyped_orders()


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    quarantine=dg.AssetOut(
        description="Every row, because every row was rejected. This one is written; the good table is not."
    ),
)
def doomed_orders() -> pl.DataFrame:
    """Three lines, all of them negative, with a quarantine declared.

    Nothing survives, so the good output is skipped rather than emptied, the quarantine takes all three rows, the checks fail at `ERROR`, and the run raises `NothingSurvivedError`. An empty table that used to hold data is the quietest possible pipeline failure, so the package refuses to write one.
    """
    return _data.hopeless_orders()
