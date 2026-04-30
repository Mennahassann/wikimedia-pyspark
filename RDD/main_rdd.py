from pyspark.sql import SparkSession
from common.timer import measure
from RDD.task1 import Task1


if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("Wikimedia Project") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext

    path = "data/data.out"

    print("\n TASK 1 RDD :")

    measure("RDD Task1", lambda: Task1().run(sc, path))

    spark.stop()