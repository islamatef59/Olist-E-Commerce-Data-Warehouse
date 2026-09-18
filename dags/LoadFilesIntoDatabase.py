import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import ETL_Pipline

load_dotenv()

# Access environment variables
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "user")
DB_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "olist")

tables = ETL_Pipline.build_star_schema()


def load_tables_to_postgres(USER, PASSWORD, HOST, PORT, DBNAME, tables_data):
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
    engine = create_engine(DATABASE_URL)

    tables_dict = {
        "dim_customers": tables_data["dim_customers"],
        "dim_seller": tables_data["dim_seller"],
        "dim_products": tables_data["dim_products"],
        "dim_date": tables_data["dim_date"],
        "dim_payments": tables_data["dim_payments"],
        "fact_orders": tables_data["fact_orders"],
        "fact_orders_items": tables_data["fact_order_items"],
        "fact_payments": tables_data["fact_payments"],
        "fact_reviews": tables_data["fact_reviews"],
    }

    # Open connection context properly
    with engine.begin() as conn:
        # Test database connection
        conn.execute(text("SELECT 1"))
        print(f"PostgreSQL connection to '{DBNAME}' successful.")

        # Optional manual drop inside valid connection scope
        for table_name in tables_dict.keys():
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

    # Load tables using pandas engine connection
    for table_name, df in tables_dict.items():
        print(f"Loading {table_name}...")
        df.to_sql(
            table_name,
            con=engine,
            if_exists="append",  # Automatically handles drop + create
            index=False,
            chunksize=5000,
            method="multi"
        )
        print(f"Table {table_name} loaded successfully.")

    print("All tables loaded successfully.")


if __name__ == "__main__":
    load_tables_to_postgres(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, tables)