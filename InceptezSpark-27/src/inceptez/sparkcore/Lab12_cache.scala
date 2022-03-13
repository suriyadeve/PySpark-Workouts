package inceptez.sparkcore
import org.apache.spark.SparkContext


object Lab12_cache {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(master = "local",appName= "Lab12")
    sc.setLogLevel("ERROR")
    
    val tdata = sc.textFile("file:/home/hduser/hive/data/txns")
    
    val tdata1 = tdata.map(line => line.split(","))
    tdata1.cache()
    
    val tdata2 = tdata1.filter(arr => arr(7) == "California" && arr(8) == "cash")
    
    tdata2.cache()
    
    tdata2.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    
    tdata2.persist()
    
    println(s"No of Transaction in california with payment as cash: ${tdata2.count}")
    
    val tdata3 = tdata2.map(row => List(row(0),row(3),row(6),row(7)))
    
    tdata3.foreach(println)
    
    tdata2.unpersist()
    
    val tdata4 = tdata1.filter(arr => arr(7) == "Texas" && arr(8) == "credit")
    
    println(s"No of transaction in texas with payment  as credit: ${tdata4.count}")
    
    val tdata5 = tdata4.map(row => List(row(0),row(3),row(6),row(7)))
    
    tdata5.foreach(println)
    
    
    
  }
}