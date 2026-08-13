"""Groups `failure/*`: the four exits a run can take, one subgroup each.

The library's failure-policy table has five rows and four of them are here. The fifth is `catalog/orders`, where every row is valid and the question never arises.

Declaring a quarantine **is** the consent to partial data, so what an invalid row costs is visible in the definition and cannot disagree with what the asset declares. `failure/quarantine` and `failure/no_quarantine` are the same twenty rows either side of that one argument, which is why they read from the same base table.

**Three of these fail on every run, deliberately.** A red shape check and an aborted run are surfaces too, and each raises a different error from the package with a different thing to say. Keep them out of a bulk materialize, or the aborted run buries the assets you wanted populated.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo.schema import Orders


@dd.dataframely_asset(
    schema=Orders,
    group_name="failure/quarantine",
    description=(
        "Twenty lines, eight of them invalid, with `quarantine=dg.AssetOut()` declared.\n\n"
        "The eight invalid lines land in the sibling asset, the checks that rejected them fail "
        "at `WARN`, and the run stays green so downstream proceeds on the data that is fine. "
        "Each red check carries up to five of the rows it rejected.\n\n"
        "Compare its check list against `strict_orders`, which is the same data with no "
        "quarantine declared."
    ),
    quarantine=dg.AssetOut(
        description=(
            "The rows `Orders` rejected: the original columns, then one `String` column per rule "
            "reading `valid`, `invalid` or `unknown` and named exactly as that rule's asset check. "
            "Its materialization also carries a `cooccurrence` table, so the one row that tripped "
            "three rules reads as one row rather than three unrelated counts.\n\n"
            "Its only parent is `quarantined_orders`, not the base table both came from."
        ),
    ),
)
def quarantined_orders(defective_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The middle exit: survivors written, the rest inspectable next door."""
    return defective_raw_orders


@dd.dataframely_asset(
    schema=Orders,
    group_name="failure/no_quarantine",
    description=(
        "The same twenty lines as `quarantined_orders` with no quarantine declared, so the run "
        "fails and writes nothing.\n\n"
        "Without a quarantine every row has to be valid. The checks fail at `ERROR`, the run "
        "raises `ValidationAbortError`, and the last-known-good table stays in place. Landing the "
        "survivors and dropping the rest is the failure this package exists to make visible, so "
        "it is not reachable by configuration."
    ),
)
def strict_orders(defective_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The abort exit: rows rejected with nowhere to route them."""
    return defective_raw_orders


@dd.dataframely_asset(
    schema=Orders,
    group_name="failure/shape",
    description=(
        "Clean rows arriving from `mistyped_raw_orders` with `quantity` as `Int64` where the "
        "schema declares `Int32`.\n\n"
        "The blocking `dy_schema__dtypes` check fails and the run raises `SchemaShapeError` "
        "before a single row is filtered, so no rule check reports at all. The package never "
        "casts: silently widening a dtype is how a thousandfold error reaches a table nobody "
        "re-reads."
    ),
)
def mistyped_orders(mistyped_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The shape exit: a pipeline defect rather than a data one."""
    return mistyped_raw_orders


@dd.dataframely_asset(
    schema=Orders,
    group_name="failure/nothing_survives",
    description=(
        "Three lines, all of them negative, with a quarantine declared.\n\n"
        "Nothing survives, so the valid output is skipped rather than emptied, the quarantine "
        "takes all three rows, the checks fail at `ERROR`, and the run raises "
        "`NothingSurvivedError`. An empty table where data used to be is the quietest possible "
        "pipeline failure, so the package refuses to write one: consenting to partial data was "
        "never consent to no data."
    ),
    quarantine=dg.AssetOut(
        description="Every row, because every row was rejected. This one is written; the valid table is not."
    ),
)
def doomed_orders(hopeless_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The nothing-survived exit."""
    return hopeless_raw_orders
