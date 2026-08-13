"""Smoke tests: the code location loads and still holds what the README claims.

A demo's failure mode is silent rot. The library moves, `dg` autoloading picks up a module that no longer defines what it used to, and nobody notices until the webserver is open in front of an audience. Asserting the counts here is what turns that into a red CI run instead.
"""

import collections
import datetime as dt
import inspect
from pathlib import Path

import dagster as dg
import polars as pl
import pytest

import dagster_dataframely_demo
from dagster_dataframely_demo import _data
from dagster_dataframely_demo.defs.metadata import annotated_orders
from dagster_dataframely_demo.schema import Orders

EXPECTED_GROUPS = {
    "a_catalog": 3,
    "b_quarantine": 3,
    "c_failures": 4,
    "d_granularity": 3,
    "e_storage": 4,
    "f_partitions": 4,
    "g_metadata": 3,
    "h_wiring": 3,
}

# What each `check_granularity` collapses the schema's rules down to, counting the
# blocking `dy_schema__dtypes` shape check. These are the numbers the README quotes.
EXPECTED_CHECKS = {
    "orders": 24,
    "orders_by_rule": 24,
    "orders_by_column": 14,
    "orders_by_schema": 2,
}

# Every rule the defective frame is built to break, one per row bar the two that
# share `ORD-0015` and the two lines of `ORD-0016`.
EXPECTED_BROKEN_RULES = {
    "amount|min",
    "email|check__lowercase",
    "line_numbers_are_dense",
    "order_id|regex",
    "paid_orders_have_amount",
    "priority|is_in",
    "quantity|max",
}


@pytest.fixture(scope="module")
def defs() -> dg.Definitions:
    """Loads the code location exactly as `dg dev` does."""
    return dg.load_from_defs_folder(
        path_within_project=Path(dagster_dataframely_demo.__file__).parent
    )


def _definitions(defs: dg.Definitions) -> list[dg.AssetsDefinition]:
    """The assets, narrowed to the one member of the union that carries specs.

    `Definitions.assets` is typed to hold source assets and cacheable ones too, and neither has a `specs` or a `check_specs`. This project declares only real ones, so the filter is a type narrowing rather than a behaviour.
    """
    return [a for a in defs.assets or [] if isinstance(a, dg.AssetsDefinition)]


def _spec(defs: dg.Definitions, key: str) -> dg.AssetSpec:
    return next(
        spec
        for asset in _definitions(defs)
        for spec in asset.specs
        if spec.key.to_user_string() == key
    )


def test_every_group_holds_the_assets_the_readme_lists(defs: dg.Definitions):
    counts = collections.Counter(
        spec.group_name for asset in _definitions(defs) for spec in asset.specs
    )
    assert dict(counts) == EXPECTED_GROUPS


def test_both_io_managers_are_bound(defs: dg.Definitions):
    assert sorted(defs.resources or {}) == ["csv_io_manager", "io_manager"]


def test_check_granularity_collapses_the_rules_as_documented(defs: dg.Definitions):
    counts = collections.Counter()
    for asset in _definitions(defs):
        for spec in asset.check_specs or []:
            counts[spec.asset_key.to_user_string()] += 1
    assert {key: counts[key] for key in EXPECTED_CHECKS} == EXPECTED_CHECKS


def test_the_defective_frame_breaks_the_rules_the_demo_advertises():
    """`b_quarantine` and `c_failures` are only worth looking at if the data still fails."""
    valid, failure = Orders.filter(_data.defective_orders(), cast=False)
    assert valid.height == 12
    assert len(failure) == 8
    assert {
        rule for rule, count in failure.counts().items() if count
    } == EXPECTED_BROKEN_RULES


def test_the_partitions_hold_disjoint_orders():
    """The fan-in in `f_partitions` is only valid while no order is split across two days.

    Restamping every line onto every day would duplicate the primary key the moment `orders_rollup` concatenated two partitions, and split an order across days would take out `line_numbers_are_dense` for both halves.
    """
    days = [dt.date(2026, 8, day) for day in range(1, 6)]
    combined = pl.concat(_data.orders_on(day) for day in days)

    assert combined.height == _data.clean_orders().height
    _, failure = Orders.filter(combined, cast=False)
    assert len(failure) == 0


def test_the_undescribed_asset_shows_the_schemas_docstring(defs: dg.Definitions):
    """The one asset that passes no `description=`, so the fallback is visible in the UI.

    `cleandoc` because that is what the decorator applies: a raw docstring keeps its source indentation, which the catalog would render as a code block.
    """
    assert _spec(defs, "orders_undescribed").description == inspect.cleandoc(
        Orders.__doc__ or ""
    )


def test_the_described_assets_show_their_own_prose(defs: dg.Definitions):
    """Every other asset passes `description=`, or the whole demo would read as one sentence."""
    described = [
        spec
        for asset in _definitions(defs)
        for spec in asset.specs
        if spec.description == inspect.cleandoc(Orders.__doc__ or "")
    ]
    assert [spec.key.to_user_string() for spec in described] == ["orders_undescribed"]


def test_a_returned_result_is_inspectable_by_calling_the_asset():
    """Direct invocation, which is what `g_metadata` claims and what a user's own tests would do."""
    events = list(annotated_orders(_data.clean_orders()))  # pyrefly: ignore[bad-argument-type]
    materialization = next(
        event for event in events if isinstance(event, dg.MaterializeResult)
    )
    metadata = materialization.metadata or {}

    assert materialization.value.height == 12
    assert metadata["source"] == "stripe"
    # The package counts the valid rows itself and applies that key last, so the 999 loses.
    assert metadata["dagster/row_count"] == 12
    assert materialization.data_version == dg.DataVersion("2026-08-05")
