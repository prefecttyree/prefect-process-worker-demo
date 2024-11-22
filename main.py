
from prefect import flow, task
from datetime import timedelta
import random
import time

@task(name="process_data", retries=2, retry_delay_seconds=30)
def process_data(data):
    """Task to simulate data processing with potential failures"""
    if random.random() < 0.2:  # 20% chance of failure
        raise Exception("Random processing error!")
    
    time.sleep(2)  # Simulate processing time
    return f"Processed data: {data * 2}"

@task(name="validate_result")
def validate_result(result):
    """Task to validate the processing result"""
    return len(result) > 0

@flow(name="data_processing_flow", 
      description="Flow demonstrating task orchestration",
      retries=1,
      retry_delay_seconds=60)
def data_processing_flow(input_value: int = 10):
    """Main flow that orchestrates data processing tasks"""
    
    # Process the data
    processed_result = process_data(input_value)
    
    # Validate the result
    is_valid = validate_result(processed_result)
    print(f"Flow result: {processed_result}")
    return {"result": processed_result, "is_valid": is_valid}


if __name__ == "__main__":
    
    # Create a deployment for the flow from a Git repository source
    # .from_source() specifies where Prefect should get the flow code from
    data_processing_flow.from_source(
        # The Git repository containing the flow code
        source="https://github.com/prefecttyree/prefect-process-worker-demo.git",
        # The path to the flow function within the repository
        # Format is: file_path:function_name
        entrypoint="main.py:data_processing_flow"
    ).deploy(
        # Configure the deployment settings
        # The name that will identify this deployment in the Prefect UI
        name="data-processor",
        # The work pool that will execute this deployment
        # Must match an existing work pool name in your Prefect server
        work_pool_name="my-process-worker",
        # Tags help organize and filter deployments
        # 'local' tag indicates this is for local development
        tags=["local"]
    )