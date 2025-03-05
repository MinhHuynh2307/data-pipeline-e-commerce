from dagster import Definitions, load_assets_from_modules

from e_commerce import assets  # noqa: TID252
from dagster_duckdb import DuckDBResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)
