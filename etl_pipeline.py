from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        pd.DataFrame: customer-level analytics summary
    """
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    
    # Join orders with order_items
    merged = orders.merge(order_items, on="order_id", how="inner")

    # Join with products to get category and unit_price
    merged = merged.merge(products, on="product_id", how="inner")

    # Exclude cancelled orders
    merged = merged[merged["status"] != "cancelled"]

    # Exclude suspicious quantities
    merged = merged[merged["quantity"] <= 100]

    # Compute line total
    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    # Customer-level aggregation
    customer_summary = (
        merged.groupby("customer_id")
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("line_total", "sum")
        )
        .reset_index()
    )

    # Average order value
    customer_summary["avg_order_value"] = (
        customer_summary["total_revenue"] / customer_summary["total_orders"]
    )

       # Compute top category by revenue per customer
    category_revenue = (
        merged.groupby(["customer_id", "category"])["line_total"]
        .sum()
        .reset_index()
    )

    category_revenue = category_revenue.sort_values(
        ["customer_id", "line_total", "category"],
        ascending=[True, False, True]
    )

    top_category = (
        category_revenue.drop_duplicates(subset=["customer_id"])
        [["customer_id", "category"]]
        .rename(columns={"category": "top_category"})
    )

    # Merge customer names
    customer_names = customers[["customer_id", "customer_name"]]

    customer_summary = customer_summary.merge(customer_names, on="customer_id", how="left")
    customer_summary = customer_summary.merge(top_category, on="customer_id", how="left")

    # Reorder columns
    customer_summary = customer_summary[
        [
            "customer_id",
            "customer_name",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category"
        ]
    ]

    return customer_summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0
    - No duplicate customer_id values
    - total_orders > 0

    Args:
        df: transformed DataFrame

    Returns:
        dict: validation results

    Raises:
        ValueError: if any critical check fails
    """
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "total_revenue_positive": (df["total_revenue"] > 0).all(),
        "no_duplicate_customer_id": not df["customer_id"].duplicated().any(),
        "total_orders_positive": (df["total_orders"] > 0).all(),
    }

    for check_name, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"{check_name}: {status}")

    if not all(checks.values()):
        raise ValueError("Validation failed: one or more critical checks failed.")

    return checks


def load(df, engine, csv_path):
    """Load transformed data into PostgreSQL and CSV.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: output CSV file path
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows to customer_analytics table")
    print(f"Saved CSV to {csv_path}")


def main():
    """Run the ETL pipeline end-to-end."""
    engine = create_engine("postgresql+psycopg://postgres:postgres@localhost:5432/amman_market")

    print("Starting ETL pipeline...")

    data = extract(engine)
    print("Extraction complete:")
    print(f"  customers: {len(data['customers'])}")
    print(f"  products: {len(data['products'])}")
    print(f"  orders: {len(data['orders'])}")
    print(f"  order_items: {len(data['order_items'])}")

    transformed_df = transform(data)
    print(f"Transformation complete: {len(transformed_df)} customer rows")

    validation_results = validate(transformed_df)
    print("Validation complete.")
    print(validation_results)

    load(transformed_df, engine, "output/customer_analytics.csv")
    print("ETL pipeline finished successfully.")


if __name__ == "__main__":
    main()