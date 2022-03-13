package inceptez.sparkcore
import org.apache.spark.SparkContext

object Lab14_broadcast_join {
  
  def main(args:Array[String])=
  {
    //select c.custid,c.profession,t.state from cust c inner join txns t on c.custid = t.custid
    
    val sc = new SparkContext(master="local",appName="Lab14")
    
    sc.setLogLevel("ERROR")
    
    val custrdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    val txnsrdd = sc.textFile("file:/home/hduser/hive/data/txns")
    
    val custdata = custrdd.map(x => x.split(",")).filter(x => x.length == 5).map(x => (x(0),x(4)))
    
    val custdata1 = custdata.collectAsMap()
    
    val bccustdata = sc.broadcast(custdata1)
    
    //println(bccustdata)
    
    val txnsrdd1 = txnsrdd.map(x => x.split(",")).map(x => (x(2),x(7))).filter(x => x._2.toLowerCase() == "california")
        
    //val joindata = txnsrdd1.join(custrdd1)
    
    val joindata = txnsrdd1.map(trans => (trans._1,trans._2,bccustdata.value.get(trans._1).getOrElse("")))
    
    joindata.foreach(println)
    
  }
}