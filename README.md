# E-Commerce Data Pipeline Documentation
## Overview
This data pipeline processes and analyzes e-commerce transaction data to generate business insights. The pipeline is built using Dagster, a data orchestration framework, and DuckDB for data processing and storage.
## Architecture
The pipeline follows a three-stage ETL (Extract, Transform, Load) process:
1. **Ingestion**: Raw data is loaded from CSV files into DuckDB tables
2. **Cleaning**: Data is validated and cleaned to ensure quality
3. **Analysis**: Business metrics are calculated and made available for reporting
## Data Flow Diagram

## Technologies Used
1. **Dagster**: Data orchestration framework for defining assets and dependencies
2. **DuckDB**: In-process SQL OLAP database for data processing
3. **Pandas**: Data manipulation and analysis
4. **Python**: Core programming language
## Pipeline Components
### Ingestion Assets
These assets load raw data from CSV files into DuckDB tables:
+ `customers`: Loads customer data
+ `orders`: Loads order transaction data
+ `items`: Loads item/product data
### Cleaning Assets
These assets perform data validation and cleaning:
+ `cleaned_customers`: Filters out records with null IDs or emails
+ `cleaned_orders`: Filters out records with null IDs or customer IDs
+ `cleaned_items`: Filters out records with null order IDs, item IDs, or gift card status
### Analysis Assets
These assets generate business insights:
+ `top_selling_items`: Identifies the top 50 products by revenue
+ `most_purchased_customers`: Identifies the top 50 customers by purchase frequency
+ `total_revenue_by_category`: Calculates revenue by product category (using SKU)
## Assumptions
1. Data Quality: The pipeline assumes that most records have valid IDs and key fields, with cleaning steps to filter out invalid records.
2. Categorization: The pipeline uses SKU values as a proxy for product categories, assuming SKUs follow a consistent pattern.
3. Revenue Calculation: Revenue is calculated as price × quantity, without accounting for discounts in the current implementation.
4. Cancelled Orders: Orders with a non-null CANCELLED_AT value are excluded from analysis.
5. Data Freshness: The pipeline assumes batch processing of data, not real-time streaming.
## Usage
The pipeline is designed to be run using Dagster's orchestration capabilities. Assets can be materialized individually or as a complete pipeline.
To run the full pipeline:
```bash
materialize
```