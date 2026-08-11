"""Where the demo's tables land.

Both managers take a universal-pathlib `base_dir`, so the same code writes to a local directory or to cloud storage. `DEMO_STORAGE_DIR=s3://my-bucket/demo` is the whole change, given `s3fs` installed alongside.

The default is relative, which resolves against the working directory. `dg dev` is meant to be run from the project root, so the tables land in `storage/` beside `pyproject.toml`.
"""

import os

import dagster as dg
import dagster_dataframely as dd

#: Root for both managers. Relative by default; override to write somewhere else, including a cloud URI.
STORAGE_DIR = os.environ.get("DEMO_STORAGE_DIR", "storage")


@dg.definitions
def resources() -> dg.Definitions:
    """Binds the package's two IO managers.

    These are the supported storage path. They record `path`, `bytes_written` and `dagster/storage_kind` on each materialization and nothing else: the asset definition owns what the data is, and leaving the materialization bucket empty is what keeps the Columns tab showing the schema as declared.
    """
    return dg.Definitions(
        resources={
            "io_manager": dd.DataframelyParquetIOManager(
                base_dir=f"{STORAGE_DIR}/parquet"
            ),
            "csv_io_manager": dd.DataframelyCSVIOManager(base_dir=f"{STORAGE_DIR}/csv"),
        }
    )
