from pyspark.sql import SparkSession
from common.timer import measure

from RDD.task1 import Task1
from RDD.task2 import Task2MapReduce
from RDD.task4 import Task4RDD
from RDD.task5 import Task5
from Loops.task1 import Task1Loops
from Loops.task2 import Task2Loops
from Loops.task4 import Task4Loops
from Loops.task5 import Task5Loops


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
    measure("RDD Task2", lambda: Task2MapReduce().run(sc, path))
    measure("RDD Task4", lambda: Task4RDD().run(sc, path))
    measure("RDD Task5", lambda: Task5().run(sc, path))

    print("\nLOOP TASKS:\n")
    measure("Loop Task1", lambda: Task1Loops().run(path))
    measure("Loop Task2", lambda: Task2Loops().run(path))
    measure("Loop Task4", lambda: Task4Loops().run(path))
    measure("Loop Task5", lambda: Task5Loops().run(path))

    spark.stop()