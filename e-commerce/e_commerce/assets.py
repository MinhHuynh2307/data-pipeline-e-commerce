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
            create or replace view cleaned_customers as (
                select 
                    ID,
                    EMAIL,
                    FIRST_NAME,
                    LAST_NAME,
                    PHONE,
                    STREET_ADDRESS,
                    CITY,
                    STATE,
                    ZIP_CODE,
                    COUNTRY,
                    JOIN_DATE,
                    STATUS
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
                select 
                    ID,
                    CUSTOMER__ID,
                    ORDER_DATE,
                    STATUS,
                    TOTAL_AMOUNT,
                    TOTAL_TAX,
                    TOTAL_SHIPPING,
                    TOTAL_DISCOUNT,
                    TOTAL_REVENUE,
                    TOTAL_QUANTITY,
                    STREET_ADDRESS,
                    CITY,
                    STATE,
                    ZIP_CODE,
                    COUNTRY,
                    JOIN_DATE,
                    STATUS
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
                select 
                    ID,
                    ORDER_ID,
                    ITEM_ID,
                    QUANTITY,
                    UNIT_PRICE,
                    GIFT_CARD,
                    STREET_ADDRESS,
                    CITY,
                    STATE,
                    ZIP_CODE,
                    COUNTRY,
                    JOIN_DATE,
                    STATUS
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
    """
    Top-Selling Items by Revenue

    SELECT 
        i.id AS item_id,
        i.name AS item_name,
        SUM(oi.quantity * oi.unit_price) AS total_revenue,
        SUM(oi.quantity) AS total_quantity_sold
    FROM 
        items i
    JOIN 
        order_items oi ON i.id = oi.item_id
    JOIN 
        orders o ON oi.order_id = o.id
    WHERE 
        o.status = 'completed'  -- Only count completed orders
    GROUP BY 
        i.id, i.name
    ORDER BY 
        total_revenue DESC
    LIMIT 10;  -- Adjust the limit as needed
    """
    pass


@dg.asset(
    compute_kind="duckdb",
    group_name="analysis",
    deps=[cleaned_customers, cleaned_orders],
)
def most_purchased_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    Customers with Most Purchases

    SELECT 
        c.id AS customer_id,
        c.name AS customer_name,
        c.email,
        COUNT(DISTINCT o.id) AS order_count,
        SUM(o.total_amount) AS total_spent
    FROM 
        customers c
    JOIN 
        orders o ON c.id = o.customer_id
    WHERE 
        o.status = 'completed'  -- Only count completed orders
    GROUP BY 
        c.id, c.name, c.email
    ORDER BY 
        order_count DESC, total_spent DESC
    LIMIT 10;  -- Adjust the limit as needed
    """
    pass


@dg.asset(
    compute_kind="duckdb",
    group_name="analysis",
    deps=[cleaned_orders, cleaned_items],
)
def total_revenue_by_category(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    Total Revenue by Category

    SELECT 
        ic.category_name,
        SUM(oi.quantity * oi.unit_price) AS total_revenue,
        COUNT(DISTINCT o.id) AS order_count,
        COUNT(DISTINCT o.customer_id) AS customer_count
    FROM 
        item_categories ic
    JOIN 
        items i ON ic.id = i.category_id
    JOIN 
        order_items oi ON i.id = oi.item_id
    JOIN 
        orders o ON oi.order_id = o.id
    WHERE 
        o.status = 'completed'  -- Only count completed orders
    GROUP BY 
        ic.category_name
    ORDER BY 
        total_revenue DESC;
    """
    pass
