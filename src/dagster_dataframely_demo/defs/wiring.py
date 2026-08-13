"""Group `wiring`: the same surfaces, assembled by hand.

The decorator is one arrangement of parts the package also exports under `dd.wiring`. Reach for them when the decorator's shape is not the shape you need: a schema attached to an asset you did not declare, or an out arrangement the decorator does not offer.

Two assets here, and the pair is the point. The first is what the decorator builds, a `@dg.multi_asset` with two outs and `process` between them. The second gives up the quarantine and becomes a plain `@dg.asset`, which is all one out needs.

This is not a route to `dy.Collection` support. `process` is single-schema by signature, so hand-wiring a Collection means reimplementing the hardest part of the package rather than assembling it. Declare one asset per member instead.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo.schema import Orders

GROUP = "wiring"

SOURCE = "defective_raw_orders"
VALID = "hand_wired_orders"
QUARANTINE = "hand_wired_orders_quarantine"


@dg.multi_asset(
    name=VALID,
    outs={
        VALID: dg.AssetOut(
            # `is_required=False` matters on both outs: the shape check and both abort paths end
            # the step without yielding either, and a clean run skips the quarantine.
            is_required=False,
            metadata=dd.wiring.schema_metadata(Orders),
            group_name=GROUP,
            description=(
                "`@dg.multi_asset` wired by hand out of `dd.wiring.schema_metadata`, "
                "`dd.wiring.check_specs` and `dd.wiring.process`. Its Columns tab, check list and "
                "materialization metadata are the ones `quarantined_orders` has, which is the "
                "point: the decorator is assembly, not magic."
            ),
        ),
        QUARANTINE: dg.AssetOut(
            is_required=False,
            # Two entries rather than one call: `schema_metadata` carries the live schema class
            # for the CSV reader, which both tables need, and `quarantine_table_schema` overrides
            # the Columns tab with one that states no constraints. These rows are here precisely
            # for breaking them.
            metadata=dd.wiring.schema_metadata(Orders)
            | {"dagster/column_schema": dd.wiring.quarantine_table_schema(Orders)},
            group_name=GROUP,
            description="The invalid rows, on an out you declared rather than one the decorator added.",
        ),
    },
    # What hangs the quarantine off the valid table instead of off this asset's own parents,
    # which is what a `multi_asset` gives every out by default. The whole map is required:
    # name only the quarantine and Dagster refuses it, because every input the valid out holds
    # has to be accounted for, which is what the first entry is doing.
    internal_asset_deps={
        VALID: {dg.AssetKey(SOURCE)},
        QUARANTINE: {dg.AssetKey(VALID)},
    },
    check_specs=dd.wiring.check_specs(Orders, asset=VALID),
)
def hand_wired_orders(
    context: dg.AssetExecutionContext, defective_raw_orders: pl.DataFrame
) -> dd.wiring.AssetYield:
    """Runs what `dataframely_asset` runs, on outs and check specs written by hand.

    Resolve both keys with `asset_key_for_output` rather than building them: an out that declares `key_prefix` has a key its output name does not spell, and a result yielded against a key no out owns fails the step on the first yield.

    The one key that cannot be resolved that way is the one in `internal_asset_deps`, which Dagster reads while the definition is being built, before there is a context to ask.
    """
    yield from dd.wiring.process(
        Orders,
        defective_raw_orders,
        valid_key=context.asset_key_for_output(VALID),
        quarantine_key=context.asset_key_for_output(QUARANTINE),
    )


@dg.asset(
    group_name=GROUP,
    metadata=dd.wiring.schema_metadata(Orders),
    check_specs=dd.wiring.check_specs(Orders, asset="hand_wired_clean_orders"),
    description=(
        "The smallest hand-wiring there is: one out, so a plain `@dg.asset` will do.\n\n"
        "`context.asset_key` is the whole of the key resolution here, because a single-output "
        "asset has exactly one key to resolve, and nothing needs `output_required=False`: with no "
        "quarantine to write, every path that does not raise yields its one out.\n\n"
        "What it gives up is where invalid rows land, which is why this one runs on the clean "
        "frame."
    ),
)
def hand_wired_clean_orders(
    context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
) -> dd.wiring.AssetYield:
    """The Columns tab, the checks and the row filter, on an asset you declared yourself."""
    yield from dd.wiring.process(Orders, raw_orders, valid_key=context.asset_key)
