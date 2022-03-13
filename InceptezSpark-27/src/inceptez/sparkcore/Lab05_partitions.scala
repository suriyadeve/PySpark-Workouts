package inceptez.sparkcore

import org.apache.spark.SparkContext

object Lab05_partitions {
  
  def main(args:Array[String])=
  {
    
    
    //core vs partition vs task
    //By Default, number of partitions is based on number of cores
    //Based on the number of partitions, number of tasks get created
    
    val sc = new SparkContext(master="local",appName="Lab05")
    
    sc.setLogLevel("ERROR")
    
    val tdata = sc.textFile("file:/home/hduser/hive/data/txns")
    
    val tdata1 = tdata.map(x => x.split(","))
    
    val tdata2 = tdata1.filter(arr => arr(7) == "California" && arr(8) == "cash")
     
    //tdata2.foreach(println)
    
    val tdata3 = tdata2.map(x => x.toList)
    
    tdata3.foreach(println)

    
    val lstdata = tdata2.collect()
    
    val s = lstdata.toList
    s.foreach(println)
    
    println(lstdata.size)
    
    
    
  }
 

}