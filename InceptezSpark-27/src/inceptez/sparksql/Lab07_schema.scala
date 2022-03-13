package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object Lab07_schema {
  
  def main(args:Array[String])=
  {
    
    val spark = SparkSession.builder().appName("Lab07_schema").master("local").getOrCreate()
    
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    val custid = StructField("custid",IntegerType,true)
    val fname = StructField("Fname",StringType,true)
    val lname = StructField("lname",StringType,true)
    val age = StructField("age",IntegerType,true)
    val prof = StructField("Prof",StringType,true)
    val custschema = StructType(List(custid,fname,lname,age,prof))
    
    val df = spark.read.format("csv").schema(custschema).load("file:/home/hduser/hive/data/custs")
    
    df.show()
    
    df.printSchema()
    
    
    
    
    
    
  }
  
}