print("Welcome to pyspark")

from pyspark import SparkContext

sc = SparkContext(master="local",appName="Lab01_Workout")

cust = sc.textFile("file:/home/hduser/hive/data/custs")

cust.foreach(print)




