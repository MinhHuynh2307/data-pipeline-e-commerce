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
    

@dg.asset(
    compute_kind="duckdb",
    group_name="cleaning",
    deps=[customers],
)
def cleaned_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table cleaned_customers as (
                select *
                from customers
            )
            """
        )

        preview_query = "select * from cleaned_customers limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from cleaned_customers").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
    

@dg.asset(
    compute_kind="duckdb",
    group_name="cleaning",
    deps=[orders],
)
def cleaned_orders(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view cleaned_orders as (
                select *
                from orders
            )
            """
        )

        preview_query = "select * from cleaned_orders limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from cleaned_orders").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
    

@dg.asset(
    compute_kind="duckdb",
    group_name="cleaning",
    deps=[items],
)
def cleaned_items(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view cleaned_items as (
                select *
                from items
            )
            """
        )

        preview_query = "select * from cleaned_items limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from cleaned_items").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
    

@dg.asset(
    compute_kind="duckdb",
    group_name="analysis",
    deps=[cleaned_orders, cleaned_items],
)
def top_selling_items(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view top_selling_items as (
                select 
                    i.NAME AS item_name,
                    i.SKU,
                    SUM(i.PRICE * i.QUANTITY) AS total_revenue,
                    SUM(i.QUANTITY) AS total_quantity_sold
                from items i
                join orders o on i.ORDER_ID = o.ID
                where o.CANCELLED_AT is null
                group by i.NAME, i.SKU
                order by total_revenue desc
                limit 50
            )
            """
        )

        preview_query = "select * from top_selling_items limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from top_selling_items").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )


@dg.asset(
    compute_kind="duckdb",
    group_name="analysis",
    deps=[cleaned_customers, cleaned_orders],
)
def most_purchased_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view most_purchased_customers as (
                select 
                    c.ID AS customer_id,
                    c.EMAIL AS customer_email,
                    count(distinct o.ID) AS order_count,
                    sum(o.TOTAL_PRICE_USD) AS total_spent
                from customers c
                join orders o on c.ID = o.CUSTOMER__ID
                where o.CANCELLED_AT is null
                group by c.ID, c.EMAIL
                order by order_count desc, total_spent desc
                limit 50
            )
            """
        )

        preview_query = "select * from most_purchased_customers limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from most_purchased_customers").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )



@dg.asset(
    compute_kind="duckdb",
    group_name="analysis",
    deps=[cleaned_orders, cleaned_items],
)
def total_revenue_by_category(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view total_revenue_by_category as (
                select 
                    i.SKU,
                    SUM(i.PRICE * i.QUANTITY) AS total_revenue,
                from items i
                join orders o on i.ORDER_ID = o.ID
                where o.CANCELLED_AT is null
                group by i.SKU
                order by total_revenue desc
            )
            """
        )

        preview_query = "select * from total_revenue_by_category limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from total_revenue_by_category").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

