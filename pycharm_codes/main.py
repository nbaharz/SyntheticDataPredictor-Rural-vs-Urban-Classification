from pyspark import SparkConf
from pyspark.sql import SparkSession
from extract2 import extract_data
from transform import transform_data
from load2 import load_data
import argparse


def get_spark_utils():
    """
    !!!DO NOT TOUCH!!!
    This function returns spark context object and spark session object.
    These are the entry point into all functionality in Spark.
    :return: SparkContext, SparkSession
    """
    conf = SparkConf().setAppName("IND_HLD"). \
        set("spark.mongodb.input.uri", "mongodb+srv://nbaharz:3253388@cluster0.isvflj4.mongodb.net/"). \
        set("spark.mongodb.output.uri", "mongodb+srv://nbaharz:3253388@cluster0.isvflj4.mongodb.net/"). \
        set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"). \
        set("spark.sql.debug.maxToStringFields", 1000)

    spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()

    sc = spark.sparkContext
    return sc, spark


if __name__ == '__main__':
    sc, spark = get_spark_utils()

    # Your Code Here
    file_name_ind = "WLD_2023_SYNTH-SVY-IND-EN_v01_M.csv"
    file_name_hld = "WLD_2023_SYNTH-SVY-HLD-EN_v01_M.csv"

    raw_data_hld = extract_data(file_name_hld)
    raw_data_ind = extract_data(file_name_ind)


    df_hld = transform_data(raw_data_hld, spark)
    df_ind = transform_data(raw_data_ind, spark)

    load_data(df_ind, df_hld, spark)
    print("finish")
    """
    This is your main function and this contains your flow.get_spark_utils function provide
    necessary variables for you like spark context.You should 
    call extract, transform?? and load functions respectively from their modules.
    
    Hint**: You may convert extracted data to RDD after that convert it to Dataframe.
    
    """

	
