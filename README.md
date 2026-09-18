Olist E-Commerce Data Warehouse & Apache Airflow ETL Pipeline

This project is an end-to-end data engineering pipeline built around the Brazilian Olist e-commerce dataset. It uses Python, Pandas, Apache Airflow, and PostgreSQL to take raw transactional CSV files, clean and transform the data, and load it into a structured Star Schema data warehouse.

The main goal of the project is to turn the original Olist datasets into a format that is easier to query and use for analytics and reporting.

🏗️ Data Warehouse Architecture

The transformed data is organized into dimension and fact tables. Surrogate keys are used for the dimensions, while additional calculations are performed to create useful business metrics.

Dimension Tables
dim_customers – Contains customer information such as the customer ID, city, and state.
dim_products – Stores product information, including physical dimensions, weight, product category, and English translations of the original Portuguese category names.
dim_seller – Contains seller information and their geographic location.
dim_payments – Provides a simple classification of payment methods.
dim_date – A calendar dimension containing details such as year, quarter, month, day, weekday, and whether the date falls on a weekend.
Fact Tables
fact_orders – Stores order-level information and delivery-related metrics, including delivery time, delays, and important order dates.
fact_order_items – Contains individual order items along with their price, freight cost, and calculated total item value.
fact_payments – Stores payment transactions, payment methods, values, installment counts, and whether a payment was made in installments.
fact_reviews – Contains customer review information and metrics related to how quickly reviews were submitted or responded to.
📂 Project Structure

The repository is organized into raw resources, Airflow DAGs, and supporting scripts:

.
├── Resources/                       # Raw Olist CSV files
│   ├── olist_orders_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
├── dags/
│   ├── ETL_Pipline.py              # Data transformation logic
│   ├── LoadDataIntoCsv.py          # Exports transformed data to CSV
│   ├── LoadFilesIntoDatabase.py    # Loads data into PostgreSQL
│   └── AutomatePipeline.py         # Airflow DAG definitions
└── README.md
⚡ Airflow Pipeline

Apache Airflow is used to automate and schedule the ETL process.

There are two DAGs defined in AutomatePipeline.py:

1. AutomatePipeline

This DAG is mainly used for validation. It checks whether the required resource files are available and verifies that the datasets contain data before the main pipeline runs.

2. olist_etl_bash_dag

This is the main ETL workflow. The tasks run in the following order:

run_etl_script
      ↓
  load_to_db
      ↓
  load_to_csv

run_etl_script
Runs ETL_Pipline.py, which reads the raw Olist datasets and performs the required cleaning and transformations.

load_to_db
Runs LoadFilesIntoDatabase.py to load the transformed tables into PostgreSQL. SQLAlchemy is used for the database connection and batch inserts, with a chunksize of 5000 to make the loading process more manageable.

load_to_csv
Runs LoadDataIntoCsv.py and saves the transformed tables as CSV files in:

/opt/airflow/dags/FilesLoaded/

The main DAG is configured to run hourly, allowing the complete workflow to be automated rather than having to run each script manually.

🛠️ Technologies Used
Python 3.x – Main programming language
Pandas – Data cleaning and transformation
Apache Airflow – Workflow orchestration and scheduling
PostgreSQL – Data warehouse database
SQLAlchemy – Database connectivity and data loading
Docker – Provides the runtime environment for Airflow and the pipeline
🚀 Setup and Execution
1. Configure the Environment

Create a .env file in the project root, or provide the following environment variables through your Docker environment:

POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=olist

Update the values according to your PostgreSQL setup.

2. Run the Pipeline Manually

The individual scripts can also be executed without Airflow.

First, run the ETL script to build and verify the transformed Star Schema:

python dags/ETL_Pipline.py

Then load the transformed data into PostgreSQL:

python dags/LoadFilesIntoDatabase.py

Finally, export the transformed tables to CSV:

python dags/LoadDataIntoCsv.py
📌 Project Overview

Overall, this project demonstrates a complete data engineering workflow starting from raw e-commerce data and ending with a structured data warehouse.

The pipeline handles data cleaning, transformation, dimensional modeling, surrogate key generation, derived metrics, database loading, and workflow automation. Using Airflow makes it possible to run the process in a consistent and repeatable way, while PostgreSQL provides a structured environment for querying the final Star Schema.
