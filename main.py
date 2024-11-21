
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
    # Run the flow directly
    result = data_processing_flow.from_source(
        source="https://github.com/prefecttyree/prefect-process-worker-demo.git",
        entrypoint="main.py:data_processing_flow"
    ).deploy( # Create a deployment
        name="data-processor",
        work_pool_name="my-process-worker",
        tags=["local"]
    )
    
    