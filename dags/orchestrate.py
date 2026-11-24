from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
import os
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

@dag
def orchestrate():
    @task
    def ingest_cdc():
        ws = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com"),
        token=os.getenv("DATABRICKS_TOKEN")
        )

        job_trigger = ws.jobs.run_now(
            job_id="765208759529665"
        )

        while True:
            job_run = ws.jobs.get_run(run_id=job_trigger.run_id)

            
            
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:    
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5) 

        return "CDC ingestion completed successfully."  

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project2/target && rm -rf /opt/airflow/walmart_project2/logs"
    

    @task.bash
    def source_freshness():
            #Manually set the working directory using the 'cd' command before running
            return "cd /opt/airflow/walmart_project2 && dbt source freshness"

    @task.bash
    def silver_technical():
        return 'cd /opt/airflow/walmart_project2 && dbt run --select silver_t'

    @task.bash
    def silver_technical_tests():
        return 'cd /opt/airflow/walmart_project2 && dbt test --select silver_t'

    @task.bash
    def silver_business():
        return 'cd /opt/airflow/walmart_project2 && dbt test --select silver_b'

    @task.bash
    def silver_business_tests():
            return 'cd /opt/airflow/walmart_project2 && dbt test --select silver_b'

    @task.bash
    def gold_ephemeral():
        return'cd /opt/airflow/walmart_project2 && dbt run --select gold/ephemeral'
    
    @task.bash
    def gold_dimension():
        return'cd /opt/airflow/walmart_project2 && dbt snapshot'
    
    @task.bash
    def gold_facts():
        return'cd /opt/airflow/walmart_project2 && dbt run --select gold/facts'

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical() >> silver_technical_tests() >> silver_business() >> silver_business_tests() >> gold_ephemeral() >> gold_dimension() >> gold_facts()

# Instantiating the DAG
orchestrate_dag = orchestrate()
