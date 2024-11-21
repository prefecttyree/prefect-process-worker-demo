# Prefect Process Worker Demo

This demo shows how to deploy a Prefect flow to a process worker and handle failures gracefully. The flow processes input data with retries and validation.

## What's Happening
The main flow `data_processing_flow` contains two tasks:
- `process_data`: Simulates data processing with a 20% chance of random failure
- `validate_result`: Validates the processed output

The flow includes:
- Task-level retries (2 attempts with 30s delay)
- Flow-level retry (1 attempt with 60s delay) 
- Random failure simulation to demonstrate error handling
- Result validation to ensure data quality

## Requirements
- Python 3.7+
- Prefect 2.0+
- Git repository with name matching "prefect-process-worker-demo" for deployment source
- Work pool named "my-process-worker"
- Deployment named "data-processor"

Make sure these names match exactly as shown in the code, or update the names in main.py to match your preferences.

## Getting Started
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Create a process work pool in Prefect:
   ```bash
   prefect work-pool create my-process-worker --type process
   ```
3. Start a worker:
   ```bash
   prefect worker start --pool my-process-worker
   ```
4. Run the flow to create deployment:
   ```bash
   python main.py
   ```
   This step creates a deployment in Prefect using the flow code from the Git repository. The deployment is named "data-processor" and configured to run on the "my-process-worker" work pool. This deployment configuration allows Prefect to execute the flow when triggered.
5. run the deployment:
   ```bash
   prefect deployment run 'data_processing_flow/data-processor'
   ```

The flow will deploy to your process worker and execute with full observability and error handling.

## Reference Links
- [Prefect Documentation](https://docs.prefect.io/)
- [Deploying Flows via Python](https://docs.prefect.io/v3/deploy/infrastructure-concepts/deploy-via-python)
- [Prefect Flow Retries](https://docs.prefect.io/latest/concepts/flows/#flow-retries)
- [Task Retries](https://docs.prefect.io/latest/concepts/tasks/#task-retries)

