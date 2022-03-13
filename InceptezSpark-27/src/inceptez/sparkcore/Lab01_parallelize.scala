package inceptez.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Lab01_parallelize {
  
  def main(args:Array[String])=
  {
    
   val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Lab01") )
   
   
  

  val rdd = sc.parallelize(List(10,20,30,15,26,80))



  println(s"Sum of Numbers: ${rdd.sum()}")

  println(s"Max of Numbers: ${rdd.max()}")

  println(s"Min of Numbers: ${rdd.min()}")
  
  println(s"Max of Numbers: ${rdd.count()}")
  
  println(s"First Element: ${rdd.first()}")
  
  val lst = rdd.take(3)
  
  print(lst.toList)
  
  val m = rdd.mean()
  
  print(s"Mean of Numbers: ${m}")
  
  rdd.foreach(println)
  
  print("======================")
  
  val rdd1 = rdd.map(x => x + 5)
  
  val rdd2 = rdd1.filter(x => x % 2 != 0)
  
  rdd2.foreach(println)
  
  //parallelize -> map -> filter -> print
  
  
  def add(a:Int,b:Int)=
      println(s"sum of $a + $b = ${a + b}")
  
  add(10, 20)

}
  
}
