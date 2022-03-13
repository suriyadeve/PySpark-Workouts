package inceptez.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object Lab02_parallelize {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Lab02"))
    
    val lst = List(10,20,5,100,70)
    
    val rlst = sc.parallelize(lst)
    
    val rlst1 = rlst.filter(x => x > 50)
    
    rlst1.foreach(println)
    
  }
}