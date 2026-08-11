"""Group `d_granularity`: the same rules collapsed three ways.

Clean data throughout, so every check is green and the only thing that differs between the three is the shape of the check list itself. Open all three Checks tabs side by side.

**Changing `check_granularity` on an asset that has already run orphans its check history.** The old check names stop being reported and their timelines end where the change landed, while the new ones start empty. Nothing migrates them, so it is a choice to make before an asset ships.
"""

import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo.schema import Orders

GROUP = "d_granularity"


@dd.dataframely_asset(schema=Orders, group_name=GROUP)
def orders_by_rule(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """`check_granularity="rule"`, the default: one check per dataframely rule, each with its own timeline.

    Twenty-four checks off thirteen columns, counting the gate. Compare the length of this list against the other two in this group.
    """
    return raw_orders


@dd.dataframely_asset(schema=Orders, group_name=GROUP, check_granularity="column")
def orders_by_column(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """`check_granularity="column"`: one `dy_col__<column>` check per rule-bearing column.

    Twenty-four down to fourteen. This is what makes a forty-column schema's check list readable. The rules no single column owns land in `dy_schema__rules`, which is what `multi_column_rules` decides.
    """
    return raw_orders


@dd.dataframely_asset(schema=Orders, group_name=GROUP, check_granularity="schema")
def orders_by_schema(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """`check_granularity="schema"`: a single `dy_schema__rules` for every rule at once.

    Two checks left, and one of them is the gate. One timeline for the whole schema.
    """
    return raw_orders
