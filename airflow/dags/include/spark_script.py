from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import logging
import os

SHARED_DATA_DIR = "/opt/airflow/data" 

def main():
    if len(sys.argv) < 2:
        logging.error("Usage: spark_script.py <filename>")
        sys.exit(-1)
        
    filename = sys.argv[1]
    spark = SparkSession.builder.appName(f"Metrics for {filename}").getOrCreate()
    
    logging.info(f"✨ Spark job received and is now processing file: {filename}")
    
    local_input_path = os.path.join(SHARED_DATA_DIR, filename)
    df = spark.read.options(header=True, inferSchema=True).csv(local_input_path)
    
    departure_station_count = df.groupBy("departure_name").count().withColumnRenamed("count", "departure_count")
    return_station_count = df.groupBy("return_name").count().withColumnRenamed("count", "return_count")
    
    metrics = departure_station_count \
        .join(return_station_count, return_station_count.return_name == departure_station_count.departure_name, how = "full_outer") \
        .drop("return_name") \
        .orderBy(desc(col("departure_count"))) \
        .na.drop()
    
    print("SHOWING RESULTS")
    print(metrics.show())
    print("SHOW FINISHED")
    
    metrics_output_parent_dir = os.path.join(SHARED_DATA_DIR, "metrics")

    # 2. Create the parent directory if it doesn't already exist
    os.makedirs(metrics_output_parent_dir, exist_ok=True)
    
    # 3. Define a clean output path for this specific job's output directory
    # Note: Spark's .csv() function creates a DIRECTORY with this name
    local_output_path = os.path.join(metrics_output_parent_dir, filename)
    
    # 4. Now, the write operation will succeed
    metrics.toPandas().to_csv(local_output_path, index=False)
    
    spark.stop()
    
    
    logging.info("✅ Spark job finished.")

if __name__ == "__main__":
    main()
    
