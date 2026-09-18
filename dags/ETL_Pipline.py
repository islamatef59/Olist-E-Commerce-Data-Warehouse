import pandas as pd


def build_star_schema():
    # -------------------------------------------------------------
    # 1. Load Source Data
    # -------------------------------------------------------------
    orders = pd.read_csv("/opt/airflow/Resources/olist_orders_dataset.csv", parse_dates=[
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ])
    reviews = pd.read_csv("/opt/airflow/Resources/olist_order_reviews_dataset.csv", parse_dates=[
        "review_creation_date",
        "review_answer_timestamp"
    ])
    customers = pd.read_csv("/opt/airflow/Resources/olist_customers_dataset.csv")
    products = pd.read_csv("/opt/airflow/Resources/olist_products_dataset.csv")
    translations = pd.read_csv("/opt/airflow/Resources/product_category_name_translation.csv")
    sellers = pd.read_csv("/opt/airflow/Resources/olist_sellers_dataset.csv")
    payments = pd.read_csv("/opt/airflow/Resources/olist_order_payments_dataset.csv")
    order_items = pd.read_csv("/opt/airflow/Resources/olist_order_items_dataset.csv",
                              parse_dates=['shipping_limit_date'])

    # -------------------------------------------------------------
    # 2. Build DimDate (Normalized to Date Midnight)
    # -------------------------------------------------------------
    all_dates = pd.concat([
        orders['order_purchase_timestamp'].dt.normalize(),
        orders['order_approved_at'].dt.normalize(),
        orders['order_delivered_carrier_date'].dt.normalize(),
        orders['order_delivered_customer_date'].dt.normalize(),
        orders['order_estimated_delivery_date'].dt.normalize(),
        reviews['review_creation_date'].dt.normalize(),
        reviews['review_answer_timestamp'].dt.normalize(),
        order_items['shipping_limit_date'].dt.normalize()
    ])

    unique_dates = pd.Series(all_dates.dropna().unique()).sort_values().reset_index(drop=True)

    dim_date = pd.DataFrame({
        'date_id': unique_dates.dt.strftime("%Y%m%d").astype(int),
        'date': unique_dates,
        'year': unique_dates.dt.year,
        'quarter': unique_dates.dt.quarter,
        'month': unique_dates.dt.month,
        'month_name': unique_dates.dt.month_name(),
        'day': unique_dates.dt.day,
        'weekday': unique_dates.dt.weekday + 1,
        'weekday_name': unique_dates.dt.day_name(),
        'is_weekend': unique_dates.dt.weekday >= 5
    })

    # Date Lookup Map (Normalized Date -> date_id)
    date_map = dim_date.set_index("date")["date_id"]

    # -------------------------------------------------------------
    # 3. Build Dimensions
    # -------------------------------------------------------------
    # DimCustomers
    dim_customers = customers.drop_duplicates(subset=["customer_id"]).reset_index(drop=True)
    dim_customers['customer_sk'] = dim_customers.index + 1
    dim_customers["customer_city"] = dim_customers["customer_city"].str.title()
    dim_customers["customer_state"] = dim_customers["customer_state"].str.upper()
    dim_customers = dim_customers[[
        "customer_sk", "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state"
    ]]

    # DimProducts
    products = products.merge(translations, how="left", on="product_category_name")
    dim_products = products.drop_duplicates(subset=["product_id"]).reset_index(drop=True)
    dim_products["product_category_name_english"] = dim_products["product_category_name_english"].fillna("unknown")
    dim_products['product_sk'] = dim_products.index + 1
    dim_products = dim_products[[
        "product_sk", "product_id", "product_category_name", "product_category_name_english",
        "product_name_lenght", "product_description_lenght", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]]

    # DimSeller
    dim_seller = sellers.drop_duplicates(subset=['seller_id']).reset_index(drop=True)
    dim_seller['seller_sk'] = dim_seller.index + 1
    dim_seller['seller_city'] = dim_seller['seller_city'].str.title()
    dim_seller['seller_state'] = dim_seller['seller_state'].str.upper()
    dim_seller = dim_seller[[
        "seller_sk", "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
    ]]

    # DimPayments
    dim_payments = payments[["payment_type"]].drop_duplicates().reset_index(drop=True)
    dim_payments["payment_sk"] = dim_payments.index + 1

    # -------------------------------------------------------------
    # 4. Build Fact Tables
    # -------------------------------------------------------------
    # FactReviews
    fact_reviews = reviews.copy()
    fact_reviews['review_creation_date_id'] = fact_reviews['review_creation_date'].dt.normalize().map(date_map)
    fact_reviews['review_answer_timestamp_id'] = fact_reviews['review_answer_timestamp'].dt.normalize().map(date_map)
    fact_reviews = fact_reviews[[
        "review_id", "order_id", "review_score", "review_comment_title",
        "review_comment_message", "review_creation_date_id", "review_answer_timestamp_id"
    ]]

    # FactOrders
    fact_orders = orders.merge(dim_customers[["customer_id", "customer_sk"]], on="customer_id", how="left")
    fact_orders['purchase_date_id'] = fact_orders['order_purchase_timestamp'].dt.normalize().map(date_map)
    fact_orders['approved_date_id'] = fact_orders['order_approved_at'].dt.normalize().map(date_map)
    fact_orders['delivered_carrier_date_id'] = fact_orders['order_delivered_carrier_date'].dt.normalize().map(date_map)
    fact_orders['delivered_customer_date_id'] = fact_orders['order_delivered_customer_date'].dt.normalize().map(
        date_map)
    fact_orders['estimated_delivery_date_id'] = fact_orders['order_estimated_delivery_date'].dt.normalize().map(
        date_map)
    fact_orders["delivery_days"] = (
                orders["order_delivered_customer_date"] - orders["order_purchase_timestamp"]).dt.days
    fact_orders["delay_days"] = (
                orders["order_delivered_customer_date"] - orders["order_estimated_delivery_date"]).dt.days

    fact_orders = fact_orders[[
        "order_id", "customer_sk", "order_status", "purchase_date_id",
        "approved_date_id", "delivered_carrier_date_id", "delivered_customer_date_id",
        "estimated_delivery_date_id", "delivery_days", "delay_days"
    ]]

    # FactOrderItems
    fact_order_items = order_items.merge(
        dim_products[['product_sk', 'product_id']], on="product_id", how="left"
    ).merge(
        dim_seller[["seller_sk", "seller_id"]], on="seller_id", how="left"
    )
    fact_order_items['shipping_limit_date_id'] = fact_order_items['shipping_limit_date'].dt.normalize().map(date_map)
    fact_order_items["product_sk"] = fact_order_items["product_sk"].fillna(0).astype(int)
    fact_order_items["seller_sk"] = fact_order_items["seller_sk"].fillna(0).astype(int)
    fact_order_items["item_total"] = fact_order_items["price"] + fact_order_items["freight_value"]
    fact_order_items = fact_order_items[[
        "order_id", "order_item_id", "product_sk", "seller_sk",
        "shipping_limit_date_id", "price", "freight_value", "item_total"
    ]]

    # FactPayments
    fact_payments = payments.merge(dim_payments, on="payment_type", how="left")
    fact_payments["is_installment"] = fact_payments["payment_installments"] > 1
    fact_payments = fact_payments[[
        "order_id", "payment_sk", "payment_installments", "payment_value", "is_installment"
    ]]

    # -------------------------------------------------------------
    # 5. Return All Tables in Dictionary
    # -------------------------------------------------------------
    return {
        "dim_customers": dim_customers,
        "dim_seller": dim_seller,
        "dim_products": dim_products,
        "dim_date": dim_date,
        "dim_payments": dim_payments,
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items,
        "fact_payments": fact_payments,
        "fact_reviews": fact_reviews,
    }


if __name__ == "__main__":
    tables = build_star_schema()
    print("Star schema built successfully. Generated tables:", list(tables.keys()))