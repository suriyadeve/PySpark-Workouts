package inceptez.sparkcore
import org.apache.spark.SparkContext

object Lab10_leftjoin {
  
  def main(args: Array[String])=
  {
    //val sc = new SparkContext(master = "local", appName = "Lab10")
    val sc = new SparkContext
    sc.setLogLevel("ERROR")
    
    val emp = sc.textFile("file:/home/hduser/sparkdata/emp.csv")
                .map(x => x.split(","))
                .map(x => (x(2),(x(0),x(1),x(3))))
                
    emp.foreach(println)
    
    val dept = sc.textFile("file:/home/hduser/sparkdata/dept.csv")
                 .map(x => x.split(",")).map(x => (x(0),x(1)))
                 
    
    println("==============================================")
    
    dept.foreach(println)
    
    val empdept = emp.leftOuterJoin(dept)
    
    
    println("==============================================join")
    
    empdept.foreach(println)
    
    
  }
  
  
  
}