from pyspark.sql import SparkSession
from common.timer import measure

from RDD.task1 import Task1
from Loops.task1 import Task1Loops


if __name__ == "__main__":
    open("results.txt", "w").close() 

    spark = SparkSession.builder \
        .appName("Wikimedia Project") \
        .master("local[2]") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()

    sc = spark.sparkContext
    path = "data/data.out"

    print("\nRDD TASKS:\n")
    measure("RDD Task1", lambda: Task1().run(sc, path))

    print("\nLOOP TASKS:\n")
    measure("Loop Task1", lambda: Task1Loops().run(path))

    spark.stop()