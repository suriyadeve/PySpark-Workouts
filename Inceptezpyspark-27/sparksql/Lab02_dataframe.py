from pyspark import SparkContext
from pyspark.sql import SQLContext


def main():
    
    sc = SparkContext(master="local", appName="lab02")
    sc.setLogLevel("ERROR")
    
    sqlc = SQLContext(sc)
    
    #df = sqlc.read.format("csv").load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    #To infer the datatype of the input. ByDefault inferShema is false
    df = sqlc.read.format("csv").option("inferSchema",True).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    #To get the structure(columnname,datatype) of a dataframe
    
    df.printSchema()
    
    df.show()
    
main()
