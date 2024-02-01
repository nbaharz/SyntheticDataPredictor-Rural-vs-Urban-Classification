from pyspark.sql import SparkSession

def load_data(individual_data, household_data,spark):
    """
    This function gets individual info dataframe and household info dataframe. It loads data to mongodb in separate collections
    but in the same database. You should be careful about the write mode and you can find the sample code in this url.
    https://docs.mongodb.com/spark-connector/current/python/write-to-mongodb/

    :param ind_data: Dataframe of Individual Data
    :param hld_info: Dataframe of Household Data
    """
    #Your Code Here

    individual_data.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .option("uri", "mongodb+srv://nbaharz:3253388@cluster0.isvflj4.mongodb.net/ITU_project.IND_dataset") \
        .save()

    household_data.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .option("uri", "mongodb+srv://nbaharz:3253388@cluster0.isvflj4.mongodb.net/ITU_project.HLD_dataset") \
        .save()

    spark.stop()