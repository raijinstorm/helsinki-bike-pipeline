from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ExampleJob").getOrCreate()
    data = spark.range(0, 100).toDF("number")
    counts = data.groupBy((data.number % 10).alias("bucket")).count()
    counts.show()
    spark.stop()
