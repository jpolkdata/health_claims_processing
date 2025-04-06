# Health Claims Processing

Using fake healthcare data as a sample use case, this project demonstrates an ETL approach to loading that data using Python. 

# Setup
Clone the repo
Create a virtual environment using `py -m venv .venv`
Switch to the virtual environment using `.\.venv\Scripts\activate` (Windows) or `source /.venv/bin/activate ` (Linux)
Install the requirements using `py -m pip install -r requirements.txt`

# Usage
To run the tests use the command `pytest test_etl_pipeline.py`

# Next Steps
To make a more scalable, highly available, and auditable pipeline, we should consider the following
- Using Spark or Flink for scalability
- Deploy on a cloud platform for high availability
- Implement logging and monitoring using Kafka, Prometheus, or Grafana

We can also improve this solution by
- Implementing parallel processing
- Adding error handling and retry mechanisms
- Using more restrictive data validation to ensure data quality before it loads to the destination