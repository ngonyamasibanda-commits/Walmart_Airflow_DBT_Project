FROM apache/airflow:3.4.0

# Install dbt-core and adapters
RUN pip install --no-cache-dir dbt-core dbt-postgres dbt-databricks
