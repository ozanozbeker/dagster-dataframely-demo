"""Group `b_quarantine`: rejected rows land in a sibling and the run stays green.

Declaring a quarantine **is** the consent to partial data, so what a rejected row costs is visible in the definition and cannot disagree with what the asset declares. `defs/failures.py` is the same rows without that declaration.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "b_quarantine"


@dg.asset(group_name=GROUP)
def defective_raw_orders() -> pl.DataFrame:
    """Twenty order lines, eight of which break a rule. The input for `quarantined_orders` and `strict_orders`."""
    return _data.defective_orders()


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    quarantine=dg.AssetOut(
        description=(
            "The rows `Orders` rejected: the original columns, then one `String` column per rule "
            "reading `valid`, `invalid` or `unknown` and named exactly as that rule's asset check. "
            "Its materialization also carries a `cooccurrence` table, so the one row that tripped "
            "three rules reads as one row rather than three unrelated counts."
        ),
    ),
)
def quarantined_orders(defective_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The same twenty lines, with `quarantine=dg.AssetOut()` declared.

    The eight rejected lines land in the sibling asset, the checks that rejected them fail at `WARN`, and the run stays green for downstream. Each red check carries up to five of the rows it rejected.

    Compare its check list against `strict_orders`, which is the same data with no quarantine declared.
    """
    return defective_raw_orders
