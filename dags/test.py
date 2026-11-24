from databricks.sdk import WorkspaceClient
import os
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
ws = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com"),
    token=os.getenv("DATABRICKS_TOKEN")
)

job_trigger = ws.jobs.run_now(
    job_id="765208759529665"
)

while True:
    job_run_id = ws.jobs.get_run(run_id=job_trigger.run_id)

    print(f"Job run state: {job_run_id.state.life_cycle_state}, result state: {job_run_id.state.result_state}")

    if job_run_id.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:    
        if job_run_id.state.result_state == "SUCCESS":
            print("Job completed successfully.")
            break
        else:
            raise Exception(f"Job failed with state: {job_run_id.state.result_state}")
            
    time.sleep(5)    
            