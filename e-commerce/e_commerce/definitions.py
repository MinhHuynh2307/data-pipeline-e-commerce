from dagster import Definitions, load_assets_from_modules

from e_commerce import assets  # noqa: TID252
from dagster_duckdb import DuckDBResource
from e_commerce.assets import (
    customers_missing_dimension_check,
    orders_missing_dimension_check,
    items_missing_dimension_check,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        customers_missing_dimension_check,
        orders_missing_dimension_check,
        items_missing_dimension_check,
    ],
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)
