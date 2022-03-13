package inceptez.sparkcore

import org.apache.spark.SparkContext


object Lab03_wholetextfile {
  
  def main(args:Array[String])=
   {
    val sc = new SparkContext(master="local",appName="Lab03")
    sc.setLogLevel("ERROR")
    
    val tdata = sc.wholeTextFiles("file:/home/hduser/sparkdata/sparkdata1")
    
    println(tdata.count())
        
    tdata.foreach(x => println(x._1))
    
    tdata.foreach(x => println(x._2))
     
    println("============================1")
    
    val tdata1 = tdata.map(x => x._1)
    
    val filenames = tdata1.collect()
    
    //val f = filenames.toList
    //println(f)
    
    //filenames.foreach(println)
    
    println(filenames.toList)
    
    val lst = List(10,20,30,"Apple")
    
    val rdd1 = sc.parallelize(lst)
    
    val lst1 = rdd1.collect()
    
    println(lst1.toList)
    
    
    
    
    
     
    
      
  }
}