"""Group `e_storage`: what the IO managers record, and what a plan does on its way to disk.

`csv_orders` and `csv_orders_readback` are a pair. Together they prove the CSV codec is an inverse rather than a lossy convenience, which is the one thing a CSV round trip cannot be trusted on without evidence.

`streamed_extract` and `validated_stream` are the other pair. Both return a `pl.LazyFrame` and both run it on the streaming engine; where they differ is what happens to the file the sink wrote.
"""

import dagster as dg
import dagster_dataframely as dd
import polars as pl

from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "e_storage"


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    io_manager_key="csv_io_manager",
    description=(
        "The same clean rows written as CSV instead of parquet.\n\n"
        "A CSV cell holds text, so `fulfilled_in` and `tags` have nowhere to land. Both are "
        "encoded on the way out, and the run log names which columns and how. The materialization "
        "carries `path`, `bytes_written` and `dagster/storage_kind`, and nothing else: the asset "
        "definition owns what the data is."
    ),
)
def csv_orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Parquet's dtypes through a format that holds text."""
    return raw_orders


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    description=(
        "The CSV read back, and the proof that the codec is an inverse.\n\n"
        "Its green `dy_schema__dtypes` check is the whole point: `fulfilled_in` came back a "
        "`Duration('us')` and `tags` a `List(String)`, not the text a plain CSV read would hand "
        "over. The decode reads each dtype off the schema carrier in `csv_orders`' definition "
        "metadata, so it costs no round trip and cannot drift."
    ),
)
def csv_orders_readback(csv_orders: pl.DataFrame) -> pl.DataFrame:
    """The round trip, asserted by the shape check rather than by a comment."""
    return csv_orders


@dg.asset(
    group_name=GROUP,
    description=(
        "A plain `@dg.asset` returning a `pl.LazyFrame`, so the plan streams straight to "
        "storage.\n\n"
        "The IO manager sinks it through the streaming engine to a local temp file, then promotes "
        "that file to `base_dir` once the plan has succeeded, so peak memory is the engine's "
        "buffers rather than the whole frame. Nothing reaches the destination until the sink "
        "worked, which is what keeps a failing plan from truncating a good table.\n\n"
        "No schema is attached here, so nothing is validated: this is the IO manager on its own."
    ),
)
def streamed_extract() -> pl.LazyFrame:
    """The write path a `LazyFrame` takes with no schema in the way."""
    return _data.clean_orders().lazy().filter(pl.col("amount") > 0)


@dd.dataframely_asset(
    schema=Orders,
    group_name=GROUP,
    description=(
        "The same lazy return, this time validated.\n\n"
        "The plan runs on the same streaming engine, but the sink lands in a staging file that is "
        "read back whole, because `Schema.filter` collects and the rules can only be evaluated "
        "over rows in memory. Peak memory is then the size of the frame the plan produced rather "
        "than the plan's own high-water mark.\n\n"
        "So the computation streams and the storage stays eager. `temp_dir` decides which disk "
        "that staging file lands on, which matters in a container where `/tmp` is the ephemeral "
        "disk."
    ),
)
def validated_stream(streamed_extract: pl.LazyFrame) -> pl.LazyFrame:
    """A lazy return under a schema: staged, read back, filtered."""
    return streamed_extract.filter(pl.col("quantity") >= 1)
