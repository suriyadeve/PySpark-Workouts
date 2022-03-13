package inceptez.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Lab01_aboutsparkk {
 
    def main(args:Array[String])=
    {
      val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Lab01"))
          
      val cust = sc.textFile("file:/home/hduser/hive/data/custs")
         
      cust.foreach(println)
      
      println("welcome to spark")
      
    }
      
}