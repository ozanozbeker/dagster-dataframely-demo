"""Group `a_catalog`: what one decorator argument puts in the catalog.

**Start here.** Two assets over identical rows, one of which declares its schema and one of which does not. Everything else in this project is a variation on the difference.

The prose these assets show in the UI is passed as `description=` rather than left in a docstring. Unset, the decorator fills the description from the schema's own docstring, which is the right default for a real project and the wrong one here: every asset in this demo would then read "A customer order line", and the point of each would be invisible. `orders_undescribed` is the one asset that leaves it unset, so the fallback is visible once.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "a_catalog"


@dg.asset(
    group_name=GROUP,
    description=(
        "Twelve clean order lines, declaring nothing about their own shape. "
        "A plain `@dg.asset`, so it is the control: open its Columns tab beside `orders` "
        "to see what the schema is worth. This one is empty until the asset has run, and "
        "then holds only what the IO manager could infer."
    ),
)
def raw_orders() -> pl.DataFrame:
    """The control: the same rows with no schema attached."""
    return _data.clean_orders()


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    owners=["team:data-platform"],
    kinds={"polars"},
    tags={"layer": "silver"},
    description=(
        "The whole integration, in one decorator argument.\n\n"
        "The Columns tab was filled in before this asset had ever run, straight from `Orders`: "
        "dtypes, descriptions, nullability, `tracking_id` marked unique, the primary key stated "
        "once at table level, and one constraint listed beside each column. `amount` carries the "
        "column tags its `metadata=` declared.\n\n"
        "Every run reports one asset check per Dataframely rule, each with its own history, "
        "behind the blocking `dy_schema__dtypes` shape check. The materialization carries a row "
        "sample and four statistics tables."
    ),
)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The schema-backed asset every other group varies."""
    return raw_orders


@dd.dataframely_asset(schema=Orders, group_name=GROUP)
def orders_undescribed(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """This docstring never reaches the UI, and that is the surface worth seeing.

    With no `description=` passed, the decorator fills it from `Orders.__doc__`, because the schema is what describes the table while a function's docstring describes the code that fills it. Dagster's own fallback to this docstring only stands where the schema has none.
    """
    return raw_orders
