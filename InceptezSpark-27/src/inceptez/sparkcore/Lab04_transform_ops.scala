package inceptez.sparkcore

import org.apache.spark.SparkContext

object Lab04_transform_ops {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(master="local",appName="Lab04")
    sc.setLogLevel("ERROR")
    
    val tdata = sc.textFile("file:/home/hduser/hive/data/txns")
    
    val tdata1 = tdata.map(line => line.split(","))
    
    val tdata2 = tdata1.filter(arr => arr(7) == "California" && arr(8) == "cash")
    
    //select * from tbltrans where state='California' and spendby='cash'
    
    tdata2.foreach(println) //array cannot to print
    
    
    val tdata3 = tdata2.map(row => List(row(0),row(3),row(6),row(7)))
    
    //select txnid,amount,city,state from tbltrans where state='California' and spendby='cash'
    
    tdata3.foreach(println)
    
    
  }
}