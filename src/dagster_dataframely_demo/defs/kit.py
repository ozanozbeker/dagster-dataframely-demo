"""Group `f_kit`: the same surfaces, assembled by hand.

The decorator is one arrangement of parts the package also exports on their own. Reach for them when the decorator's shape is not the shape you need: a schema attached to an asset you did not declare, or an out arrangement the door does not offer.

This is not a route to `dy.Collection` support. `process` is single-schema by signature, so hand-wiring a Collection means reimplementing the hardest part of the package rather than assembling it. Declare one asset per member instead.
"""

from collections.abc import Iterator

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

OUT = "hand_wired_orders"


@dg.multi_asset(
    name=OUT,
    outs={
        OUT: dg.AssetOut(
            # `is_required=False` matters: the gate and both abort paths end the step without yielding an out.
            is_required=False,
            metadata=dd.schema_metadata(Orders),
            group_name="f_kit",
            description=(
                "`@dg.multi_asset` wired by hand out of `dd.schema_metadata`, `dd.check_specs` "
                "and `dd.process`. Its Columns tab, check list and materialization metadata are "
                "the ones `orders` has, which is the point: the decorator is assembly, not magic."
            ),
        )
    },
    check_specs=dd.check_specs(Orders, asset=OUT),
)
def hand_wired_orders() -> Iterator[
    dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult
]:
    """Runs the state machine `dataframely_asset` runs, on an out and check specs written by hand.

    The docstring stays out of the UI here: `dg.AssetOut(description=...)` is more specific and wins, which is itself worth seeing once. `process` is a generator, so the body forwards rather than returns.
    """
    yield from dd.process(
        Orders,
        _data.clean_orders(),
        context=dg.AssetExecutionContext.get(),
        good_out=OUT,
    )
