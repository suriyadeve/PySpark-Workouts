package inceptez.sparksql

import org.apache.spark.sql.SparkSession

object Lab03_movies {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("lab03-Movies").master("local").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    //BY default,delimiter is comma
    val df = spark.read.format("csv")
        .option("delimiter","$")
        .option("header",true)
        .option("inferschema",true)
        .load("file:/home/hduser/sparkdata/movies.txt")
    
    
    df.show()
    df.printSchema() 
    
    
  }
  
  
}