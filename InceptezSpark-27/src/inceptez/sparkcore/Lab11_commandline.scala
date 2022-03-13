package inceptez.sparkcore
import org.apache.spark.SparkContext


object Lab11_commandline {
 def main(args:Array[String])=
 {
   
    /*
     21-01-2022
     
     spark-submit 
     --master local 
     --class incetez.sparkcore.Lab11_commandline file:/home/hduser/sparkworkouts.jar hdfs://localhost:54310/user/hduser/coursefees.txt 2
     */
   
   if(args.length == 2)
     
   {
        val filepath = args(0)
        val partitions = args(1).toInt
        
        //val sc  = new SparkContext(master = "local",appName = "Lab11")
        val sc = new SparkContext()
        val rdd = sc.textFile(filepath,partitions)
        rdd.foreach(println)
        
        
   }
   else
   {
     println("Invali Input")
   }
 }
  
}