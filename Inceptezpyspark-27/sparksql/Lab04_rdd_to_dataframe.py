from pyspark.sql import SparkSession


def main():
        
    spark = SparkSession.builder.appName("Lab04_rdd_to_dataframe").master("local").getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    
    #from pyspark import spark.implicits._
    
    rdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    rdd1 = rdd.map(lambda x : x.split(","))
    
    rdd2 = rdd1.filter(lambda x : len(x) == 5)
    
    rdd3 = rdd2.map(lambda x : (int(x[0]),x[1],x[2],int(x[3]),x[4]))
        
    df = rdd3.toDF(["custid","fname","lname","age","prof"])
     
    
    df.show()
    
    df.printSchema()
    
       
main()