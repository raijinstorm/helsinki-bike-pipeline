# helsinki-bike-pipeline

### Setup Instructions

1.  **Clone the Repository**

    ```bash
    git clone https://github.com/raijinstorm/airflow_mongodb_task.git
    cd airflow_mongodb_task
    ```

2.  **Set File Permissions (Linux Users Only)**
    This step is crucial to ensure that files created by Airflow inside the Docker container have the correct ownership on your host machine.

    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

3.  **Initialize the Airflow Database**
    This one-time command sets up the Airflow metadata database, creates a default admin user, and runs necessary migrations.

    ```bash
    docker compose up airflow-init
    ```

4.  **Launch the Application**
    This command will build the custom Airflow image (if it doesn't exist) and start all services in the background.

    ```bash
    docker compose up --build -d
    ```

5. **Set up connection to spark**
- connection_id: spark_default
- type: spark
- host: spark://spark-master:7077
- extra json:
{
    ... ,
    "master": "spark://spark-master:7077" 
}
-----