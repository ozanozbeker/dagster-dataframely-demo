"""One smoke test: the code location loads and still holds what the README claims.

A demo's failure mode is silent rot. The library moves, `dg` autoloading picks up a module that no longer defines what it used to, and nobody notices until the webserver is open in front of an audience. Asserting the counts here is what turns that into a red CI run instead.
"""

import collections
from pathlib import Path

import dagster as dg
import pytest

import dagster_dataframely_demo
from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

EXPECTED_GROUPS = {
    "a_catalog": 2,
    "b_quarantine": 3,
    "c_failures": 4,
    "d_granularity": 3,
    "e_storage": 3,
    "f_kit": 1,
}

# What each `check_granularity` collapses the schema's rules down to, counting the
# blocking `dy_schema__dtypes` gate. These are the numbers the README quotes.
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
    good, failure = Orders.filter(_data.defective_orders(), cast=False)
    assert good.height == 12
    assert len(failure) == 8
    assert {
        rule for rule, count in failure.counts().items() if count
    } == EXPECTED_BROKEN_RULES
