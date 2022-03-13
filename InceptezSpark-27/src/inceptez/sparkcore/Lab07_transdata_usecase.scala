package inceptez.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object Lab07_transdata_usecase {
  
  def main(args:Array[String])=
  {
    //val sc = new SparkContext(master="local",appName="Lab07")
    val sc =  new SparkContext()
    
    sc.setLogLevel("ERROR")
    
    val rdd1 = sc.textFile("file:/home/hduser/hive/data/txns")
    
    val rdd2 = rdd1.map(line => line.split(","))
    
    //gettotalsalesintexas(rdd2)
    
    getmaxsoldproductintexas(rdd2)
    
    //getcountandamountbystate(rdd2)
    
  }
  
  def gettotalsalesintexas(rdd:RDD[Array[String]])=
  {
    val rdd3 = rdd.filter(line => line(7).toLowerCase().contains("texas"))
        
    val rdd4 = rdd3.map(line => line(3).toFloat)
    
    val totalsales = rdd4.sum()
    
    println("Total sales in Texas:" +totalsales)
    
    
  }
  
  def getmaxsoldproductintexas(rdd:RDD[Array[String]])=
  {
    val rdd1 = rdd.filter(line => line(7).toLowerCase().contains("texas"))
    
    val rdd2 = rdd1.map(row => (row(5),1))
    
        
    val rdd3 = rdd2.reduceByKey((x,y) => x + y)
    
    val rdd4 = rdd3.sortBy(x => x._2,false,1)
    
    val maxcountrec = rdd4.first()
    
    println(s"Product ${maxcountrec._1} has maximum sales of ${maxcountrec._2}")
    
    
    
    
   }
  
  
  def getcountandamountbystate(rdd:RDD[Array[String]])=
  {
    
    //select state,sum(amount),count(txnid)  from txns group by state
    /*
     ("California",(45.25,1))
     ("California",(20.25,1))
     ("California",(50.25,1))
     
     ("California",((45.25,1),(20.25,1) (50.25,1))
     
     x = (45.25,1)
     y = (20.25,1)
     
     x._1 + y._1 = 45.25 + 20.25 = 65.50
     
     x._2 + y._2 = 1 + 1 = 2
     
     x = (65.50,2)
     
     y = (50.25,1)
     
     x._1 + y._1 = 65.50 + 50.25 = 115.75
     x._2 + y._2 = 2 + 1 = 3
     
     (115.75,3)
     
     ("California",(115.75,3))
     
     */
    val rdd1 = rdd.map(x => (x(7),(x(3).toFloat,1)))
    
    val rdd2 = rdd1.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    
    rdd2.foreach(println)
    
    //select state,sum(amount),count(txnid),sum(amount)/count(txnid) as avgamt  from txns group by state
    
     val rdd3 = rdd2.map( x => (x._1,x._2._1,x._2._2, x._2._1 / x._2._2))
     
     rdd3.foreach(println)
     
     
  }
  
}