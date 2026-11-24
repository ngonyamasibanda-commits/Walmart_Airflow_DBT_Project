from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum

@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(2026, 11, 25, tz="CAT")
)
def orchestrate():
    @task
    def ingest_cdc():
        return "CDC data ingested"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project2/target && rm -rf /opt/airflow/walmart_project2/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running
        return "cd /opt/airflow/walmart_project2 && dbt source freshness"

    @task.bash
    def silver_technical():
        return 'cd /opt/airflow/walmart_project2 && dbt run --select silver_t'

    @task.bash
    def silver_technical_tests():
        return 'cd /opt/airflow/walmart_project2 && dbt test --select silver_t'

    @task.bash
    def silver_business():
        return 'cd /opt/airflow/walmart_project2 && dbt run --select silver_b'

    @task.bash
    def silver_business_tests():
        return 'cd /opt/airflow/walmart_project2 && dbt test --select silver_b'

    gold_ephemeral = BashOperator(
        task_id='gold_ephemeral',
        bash_command='cd /opt/airflow/walmart_project2 && dbt run --select gold/ephemeral',
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        bash_command='cd /opt/airflow/walmart_project2 && dbt snapshot',
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        bash_command='cd /opt/airflow/walmart_project2 && dbt run --select gold/facts',
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical() >> silver_technical_tests() >> silver_business() >> silver_business_tests() >> gold_ephemeral >> gold_dimensions >> gold_facts

# Instantiating the DAG
orchestrate_dag = orchestrate()
