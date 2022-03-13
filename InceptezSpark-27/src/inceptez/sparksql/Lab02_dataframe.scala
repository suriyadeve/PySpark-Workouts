package inceptez.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object Lab02_dataframe {
  
  def main(args:Array[String])=
  {
    
    val sc = new SparkContext(master="local", appName="lab02")
    sc.setLogLevel("ERROR")
    
    val sqlc = new SQLContext(sc)
    
    //val df = sqlc.read.format("csv").load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    //To infer the datatype of the input. ByDefault inferShema is false
    val df = sqlc.read.format("csv").option("inferSchema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    //To get the structure(columnname,datatype) of a dataframe
    
    df.printSchema()
   
    df.show()
    
        
  }
  
  
}