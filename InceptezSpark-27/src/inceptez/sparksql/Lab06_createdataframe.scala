package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Lab06_createdataframe {
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab06_createdataframe").master("local").getOrCreate()
    
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.filter(x => x.length == 5)
    
    //Dataframe[Row]
    
    val rdd3 = rdd2.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
    val custid = StructField("Custid",IntegerType,true)
    
    val fname = StructField("Fname",StringType,true)
    
    val lname = StructField("lname",StringType,true)
    
    val age = StructField("age",IntegerType,true)
    
    val prof = StructField("prof",StringType,true)
    
    val schema = StructType(List(custid,fname,lname,age,prof))
    
    
    val df = spark.createDataFrame(rdd3, schema)
    //DataFrame[ROW[StructType[List[StructField]]]]
    
    df.show()
    
    df.printSchema()
    
    //to get schema from the dataframe
    
    val schema2 = df.schema
    
    print(schema2)
    
    
    
    
    
    
    
  }
  
}