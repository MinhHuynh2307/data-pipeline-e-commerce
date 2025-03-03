from dagster_duckdb import DuckDBResource
import pandas as pd

import dagster as dg

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table customers as (
                select * from read_csv_auto('data/interview_customers.csv')
            )
            """
        )

        preview_query = "select * from customers limit 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from customers").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )


@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def items(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table items as (
                select * from read_csv_auto('data/interview_items.csv')
            )
            """
        )

        preview_query = "select * from items limit 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from items").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
    

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def orders(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table orders as (
                select * from read_csv_auto('data/interview_orders.csv')
            )
            """
        )

        preview_query = "select * from orders limit 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from orders").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
    

@dg.asset_check(asset=customers)
def customers_missing_dimension_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        query_result = conn.execute(
            """
            select count(*) from customers
            where ID is null
            or EMAIL is null
            """
        ).fetchone()

        count = query_result[0] if query_result else 0
        return dg.AssetCheckResult(
            passed=count == 0, metadata={"missing dimensions": count}
        )
    

@dg.asset_check(asset=orders)
def orders_missing_dimension_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        query_result = conn.execute(
            """
            select count(*) from orders
            where ID is null
            or CUSTOMER__ID is null
            """
        ).fetchone()

        count = query_result[0] if query_result else 0
        return dg.AssetCheckResult(
            passed=count == 0, metadata={"missing dimensions": count}
        )
    

@dg.asset_check(asset=items)
def items_missing_dimension_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        query_result = conn.execute(
            """
            select count(*) from items
            where ORDER_ID is null
            or ID is null
            or GIFT_CARD is null
            """
        ).fetchone()

        count = query_result[0] if query_result else 0
        return dg.AssetCheckResult(
            passed=count == 0, metadata={"missing dimensions": count}
        )
