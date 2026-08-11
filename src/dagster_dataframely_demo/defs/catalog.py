"""Group `a_catalog`: what one decorator argument puts in the catalog.

**Start here.** Two assets over identical rows, one of which declares its schema and one of which does not. Everything else in this project is a variation on the difference.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "a_catalog"


@dg.asset(group_name=GROUP)
def raw_orders() -> pl.DataFrame:
    """Twelve clean order lines, declaring nothing about their own shape.

    A plain `@dg.asset`, so it is the control. Open its Columns tab beside `orders` to see what the schema is worth: this one is empty until the asset has run, and then holds only what the IO manager could infer.
    """
    return _data.clean_orders()


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    owners=["team:data-platform"],
    kinds={"polars"},
    tags={"layer": "silver"},
)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The whole integration, in one decorator argument.

    The Columns tab was filled in before this asset had ever run, straight from `Orders`: dtypes, descriptions, nullability, `tracking_id` marked unique, the primary key stated once at table level, and one pill per remaining constraint. `amount` carries the column tags its `metadata=` declared.

    Every run reports one asset check per dataframely rule, each with its own history, behind the blocking `dy_schema__dtypes` gate. The materialization carries a row sample and four statistics tables. The transform is plain polars and takes no `context`.
    """
    return raw_orders
