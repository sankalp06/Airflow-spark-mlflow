from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Test Spark") \
        .getOrCreate()

    # Print a message
    print("Spark session created successfully.")

    # Create a DataFrame
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]
    df = spark.createDataFrame(data, ["Name", "Age"])

    # Show the DataFrame
    df.show()

    

