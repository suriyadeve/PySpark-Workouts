package inceptez.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Lab01 {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(master = "local", appName = "lab01")
    sc.setLogLevel("ERROR")
    
    //Step-1: Create SQLContext object
    val sqlc = new SQLContext(sc)
    
    //Step-2: Create dataframe using sqlc(SQLContext)
    val df = sqlc.read.format("csv").load("file:/home/hduser/hive/data/custs")
    
    //display 20 records by default
    
    //df.show()
    
    //df.show(numRows=50)
    
    //df.show(50)
    
    //by default, truncate = true
    
    //df.show(numRows=50,truncate=false)
    df.show(50,false)
    
    println("welcome to spark ")
    
       
  }
  
  
  
}