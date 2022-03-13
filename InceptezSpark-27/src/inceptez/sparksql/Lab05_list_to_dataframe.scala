package inceptez.sparksql

import org.apache.spark.sql.SparkSession


object Lab05_list_to_dataframe {
  
  def main(args:Array[String])=
    
  {
    val spark = SparkSession.builder().appName("Lab05_list_to_dataframe").master("local").getOrCreate()
    val sc = spark.sparkContext
    
    sc.setLogLevel("ERROR")
    
    import spark.implicits._
    
    val lst = List(("Raja",24),("Kumar",30),("Naveen",29),("Praveen",31))
    
    val rdd = sc.parallelize(lst)
    
    val df = rdd.toDF("UserName","UserAge")
    
    df.show()
    
    df.printSchema()
    
    
    
    
  }
  
}