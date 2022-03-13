package inceptez.sparksql

import org.apache.spark.sql.SparkSession


object Lab04_rdd_to_dataframe {

def main(args:Array[String])=
{
  
  val spark = SparkSession.builder().appName("Lab04_rdd_to_dataframe").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  
  import spark.implicits._
  
  val rdd = sc.textFile("file:/home/hduser/hive/data/custs")
  
  val rdd1 = rdd.map(x => x.split(","))
  
  val rdd2 = rdd1.filter(x => x.length == 5)
  
  val rdd3 = rdd2.map(x => (x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
  
  val df = rdd3.toDF("custid","fname","lname","age","prof")
  
  //or
  
  df.show()
  
  df.printSchema()
  
   
  val rdd31 = rdd2.map(x => customer(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
  //val rdd32 = rdd31.filter(x => x.age > 50 )
  
  val df1 = rdd31.toDF()
  
  
  df1.show()
  
  df1.printSchema()
  
}
    
  }

case class customer(custid:Int,fname:String,lname:String,age:Int,prof:String)

  
