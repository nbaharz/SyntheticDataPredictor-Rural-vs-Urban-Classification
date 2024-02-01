from pyspark.sql import SparkSession

def transform_data(raw_data,spark):
    df_data = spark.createDataFrame(raw_data)

    return df_data