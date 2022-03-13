package inceptez.sparkcore
import org.apache.spark.SparkContext

object Lab15_accumalator {
def main(args:Array[String])=
{
  //select c.custid,c.profession,t.state from cust c inner join txns t on c.custid = t.custid
  
  val sc = new SparkContext(master="local",appName="Lab14")
  sc.setLogLevel("ERROR")
  
  val rdd = sc.textFile("file:/usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log")
  
  
     /* val infocnt = rdd.filter(x => x.contains("INFO")).count()
      val warncnt = rdd.filter(x => x.contains("WARN")).count()
      val errorcnt = rdd.filter(x => x.contains("ERROR")).count()*/
     
      val ainfo = sc.longAccumulator("Infocnt")
      val awarn = sc.longAccumulator("Warncnt")
      val aerror = sc.longAccumulator("Errorcnt")
      
      rdd.foreach(x => 
        {
          if(x.contains("INFO"))
            ainfo.add(1)
           else if(x.contains("WARN"))
             awarn.add(1)
           else if (x.contains("ERROR"))
             aerror.add(1)
      
        })
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>01")
        
        rdd.filter(x => x.contains("ERROR")).foreach(println)
        
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>02")
        
        rdd.filter(x => x.contains("ERROR")).distinct().foreach(println)
        
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>03")
        println("INFO count : " +ainfo.value)
        println("WARN count : " +awarn.value)
        println("ERROR count : " +aerror.value)
        
        
        
        
  
}

}