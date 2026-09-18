from operator import index

import pandas as pd
from ETL_Pipline import build_star_schema

tables = build_star_schema()
#load dim Tables into files
tables["dim_customers"].to_csv(r"/opt/airflow/dags/FilesLoaded/dim_customers.csv" , index=False)
tables["dim_date"].to_csv(r"/opt/airflow/dags/FilesLoaded/dim_date.csv",index=False)
tables["dim_seller"].to_csv(r"/opt/airflow/dags/FilesLoaded/dim_seller.csv",index=False)
tables["dim_products"].to_csv(r"/opt/airflow/dags/FilesLoaded/dim_products.csv",index=False)
tables["dim_products"].to_csv(r"/opt/airflow/dags/FilesLoaded/dim_payments.csv",index=False)

#load Fact Tables into files
tables["fact_orders"].to_csv(r"/opt/airflow/dags/FilesLoaded/fact_orders.csv" , index=False)
tables["fact_order_items"].to_csv(r"/opt/airflow/dags/FilesLoaded/fact_order_items.csv" , index=False)
tables["fact_payments"].to_csv(r"/opt/airflow/dags/FilesLoaded/fact_payments.csv" , index=False)
tables["fact_reviews"].to_csv(r"/opt/airflow/dags/FilesLoaded/fact_reviews.csv" , index=False)