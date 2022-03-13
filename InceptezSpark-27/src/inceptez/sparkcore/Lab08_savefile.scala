package inceptez.sparkcore

import org.apache.spark.SparkContext


object Lab08_savefile {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(master = "local",appName= "Lab08")
    
    sc.setLogLevel("ERROR")
    
    //val cfess = sc.textFile("hdfs:/tmp/coursefees.txt")
    
    //val cfees = sc.textFile("/tmp/coursefees.txt")
    
    val cfees = sc.textFile("hdfs://localhost:54310/tmp/coursefee.txt")
        
    val header = cfees.first()
    
    val cfees1 = cfees.filter(x => x != header)
    
    val cfees2 = cfees1.map(x => x.split("\\$"))
    
    val cfees3 = cfees2.map(x => (x(0), x(1).toInt, x(2).toInt))
    
    //val cfees4 = cfees3.map(x => (x._1,x._2,x._3,x._2 * x._3 /100, x._2 * x._3 /100 + x._2))
    
    val cfees4 = cfees3.map(x =>
      {
        val taxamount = x._2 * x._3 / 100
        val totalfees = taxamount + x._2
        (x._1 ,x._2, x._3,taxamount,totalfees)
        
      })
    val cfees5 = cfees4.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "|" + x._5)
    
    cfees5.foreach(println)
    
    val headeradd = sc.parallelize(List("CourseName|Fees|TaxInPer|TaxAmount|TotalFess"))
    
    val frdd = headeradd.union(cfees5)
    
    frdd.foreach(println)
    
    val frdd1 = frdd.repartition(1)
    
    //val frdd1 = frdff.coalesce(1)
    
    frdd1.saveAsTextFile("hdfs://localhost:54310/sparkworkouts/coursefeewithtax")
    
   
  }
  
}