from pyspark.sql import SparkSession

print("Attempting to connect to Spark Master at spark://localhost:7077...")

try:
    # Create Spark session
    spark = SparkSession.builder \
        .appName("ConnectionTest") \
        .master("spark://localhost:7077") \
        .getOrCreate()
    
    print("✓ Successfully connected to Spark Master!")
    print(f"Spark Version: {spark.version}")
    print(f"Master URL: {spark.sparkContext.master}")
    
    # Create a simple test DataFrame
    data = [("Test", 1), ("Connection", 2), ("Success", 3)]
    df = spark.createDataFrame(data, ["Message", "Number"])
    
    print("\n=== Test DataFrame ===")
    df.show()
    
    print("\n✓ Connection test completed successfully!")
    
    spark.stop()
    
except Exception as e:
    print(f"✗ Connection failed: {e}")