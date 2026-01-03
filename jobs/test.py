from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("TestJob").getOrCreate()

# Create sample data
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("Diana", 38)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

print("=== Sample DataFrame ===")
df.show()

# Perform simple aggregation
avg_age = df.agg({"Age": "avg"}).collect()[0][0]
print(f"\nAverage Age: {avg_age}")

# Write to data directory
output_path = "/opt/spark/work-dir/data/output"
df.write.mode("overwrite").parquet(output_path)
print(f"\nData written to: {output_path}")

spark.stop()
print("\nJob completed successfully!")
