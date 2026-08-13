"""Group `g_metadata`: the two ways your own metadata reaches a materialization.

The context reads the run. The return writes the materialization.

A returned `dg.MaterializeResult` is the route this package documents: it survives a direct call in a unit test, it needs no asset key, and this package's own metadata keys win a collision so a returned `dagster/row_count` cannot make the catalog state a count nothing counted. The context route works too, and carries the sharp edges the second asset here exists to show.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo.schema import Orders

GROUP = "g_metadata"


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    description=(
        "A transform returning `dg.MaterializeResult` rather than a bare frame.\n\n"
        "`value` is the frame to validate. `metadata`, `data_version` and `tags` fold onto this "
        "materialization: look for `source` and `extract/rows` beside the package's own "
        "`dagster/row_count`, and for `dagster/data_version_is_user_provided` on the event's "
        "tags.\n\n"
        "The returned `dagster/row_count` in the code below is deliberate and loses: the package "
        "applies its own keys last, because a transform that overwrote that count would make the "
        "catalog state a number nothing counted."
    ),
)
def annotated_orders(raw_orders: pl.DataFrame) -> dg.MaterializeResult[pl.DataFrame]:
    """Metadata, tags and a data version, all on the returned result."""
    return dg.MaterializeResult(
        value=raw_orders,
        metadata={
            "source": "stripe",
            "extract/rows": raw_orders.height,
            "extract/window": dg.MetadataValue.md("`2026-08-01` to `2026-08-05`"),
            # Ignored: this package counts the valid rows itself and applies that key last.
            "dagster/row_count": 999,
        },
        data_version=dg.DataVersion("2026-08-05"),
        tags={"extract/flavour": "backfill"},
    )


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    quarantine=dg.AssetOut(
        description="Aiming `add_asset_metadata` at this key is the only way to put your own metadata on a quarantine."
    ),
    description=(
        "The other route: `context.add_asset_metadata`, which needs an `asset_key=` here.\n\n"
        "An asset declaring a quarantine can materialize two keys, so Dagster refuses to guess "
        "and raises without one. The guard reads the definition's keys rather than the run's, so "
        "a clean run that skipped the quarantine raises just the same.\n\n"
        "This route also overrides the package's own keys, which is the opposite of what a "
        "returned result does: `dagster/row_count` on this materialization reads 999, and it is "
        "wrong. That is the argument for preferring the return."
    ),
)
def context_annotated_orders(
    context: dg.AssetExecutionContext, defective_raw_orders: pl.DataFrame
) -> pl.DataFrame:
    """The context route, including the collision it wins and should not."""
    context.add_asset_metadata(
        {"source": "stripe", "dagster/row_count": 999},
        asset_key=context.asset_key_for_output("context_annotated_orders"),
    )
    context.add_asset_metadata(
        {"triage/owner": "data-platform"},
        asset_key=context.asset_key_for_output("context_annotated_orders_quarantine"),
    )
    return defective_raw_orders
