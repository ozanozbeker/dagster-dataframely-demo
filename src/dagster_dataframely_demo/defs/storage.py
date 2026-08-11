"""Group `e_storage`: what the IO managers record, and one partitioned asset.

`csv_orders` and `csv_orders_readback` are a pair. Together they prove the CSV codec is an inverse rather than a lossy convenience, which is the one thing a CSV round trip cannot be trusted on without evidence.
"""

import datetime as dt

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "e_storage"

#: Four days, fixed rather than rolling, so a backfill stays small and a screenshot keeps its keys.
DAILY = dg.DailyPartitionsDefinition(start_date="2026-08-01", end_date="2026-08-05")


@dd.dataframely_asset(schema=Orders, group_name=GROUP, io_manager_key="csv_io_manager")
def csv_orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """The same clean rows written as CSV instead of parquet.

    A CSV cell holds text, so `fulfilled_in` and `tags` have nowhere to land. Both are encoded on the way out, and the run log says which columns and how. The materialization carries `path`, `bytes_written` and `dagster/storage_kind`, and nothing else.
    """
    return raw_orders


@dd.dataframely_asset(schema=Orders, group_name=GROUP)
def csv_orders_readback(csv_orders: pl.DataFrame) -> pl.DataFrame:
    """The CSV read back, and the proof that the codec is an inverse.

    Its green `dy_schema__dtypes` gate is the whole point: `fulfilled_in` came back a `Duration('us')` and `tags` a `List(String)`, not the text a plain CSV read would hand over. The decode reads each dtype off the schema carrier in `csv_orders`' definition metadata, so it costs no round trip and cannot drift.
    """
    return csv_orders


@dd.dataframely_asset(schema=Orders, group_name=GROUP, partitions_def=DAILY)
def daily_orders() -> pl.DataFrame:
    """The clean rows, restamped onto whichever day is being materialized.

    Four daily partitions. The state machine runs per partition on that partition's frame, so every check reports per partition and the Checks tab carries a status per key. The transform still takes no `context` parameter: it reaches the partition key through `dg.AssetExecutionContext.get()`, which is the one spelling a user-side `from __future__ import annotations` cannot break.
    """
    day = dt.date.fromisoformat(dg.AssetExecutionContext.get().partition_key)
    return _data.orders_on(day)
