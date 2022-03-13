package inceptez.sparkcore
import org.apache.spark.SparkContext

object Lab13_broadcast {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(master ="local",appName="Lab13")
    sc.setLogLevel("ERROR")
    
    val words =sc.broadcast(List("of","the","is","an","and"))
    
    val lst = List("learning spark is an intresting one","Hadoop is one the bigdata storage and processing tool ")
    
    val rdd = sc.parallelize(lst)
    
    val rdd1 = rdd.flatMap(x => x.split(" "))
    
    rdd1.foreach(println)
    
    val rdd2 = rdd1.filter(w => !words.value.contains(w))
    
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.......")
    rdd2.foreach(println)
    
    
  }
  
}