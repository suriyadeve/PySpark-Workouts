package inceptez.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Lab06_shuffle
{
  def main(args:Array[String])=
  {
    //val sc = new SparkContext(master = "local",appName="Lab06")
    val sc =  new SparkContext()
    sc.setLogLevel("ERROR")
    
    val rdd1:RDD[String] = sc.textFile("file:/home/hduser/sparkdata/courses.txt")
    
    
     /*
      rdd1 = RDD(
      "hadoop spark python java"
			"hive sqoop spark java"
			"scala java spark"
			"hbase kafka java"
			)
      */
    
    val rdd2:RDD[Array[String]] = rdd1.map(line => line.split(" "))
     /*
     rdd2 =  RDD(
      Array("hadoop","spark","python","java")
			Array("hive"," sqoop","spark","java")
			Array("scala","java","spark")
			Array("hbase","kafka","java")
			)
      */
    
    val rdd3:RDD[String] = rdd2.flatMap(arr => arr)
    
    /*
     rdd3 =  RDD(
      "hadoop"
      "spark"
      "python"
      "java"
      "hive"
      "sqoop"
      "spark"
      "java"
			"scala"
			"java"
			"spark"
			"hbase"
			"kafka"
			"java")
			)
      */
    
    val rdd4:RDD[(String,Int)] = rdd3.map(word => (word,1))
    
    
    
     /*
     rdd4 =  RDD(
      ("hadoop",1)
      ("spark",1)
      ("python",1)
      ("java",1)
      ("hive",1)
      ("sqoop",1)
      ("spark",1)
      ("java",1)
			("scala",1)
			("java",1)
			("spark",1)
			("hbase",1)
			("kafka",1)
			("java",1)
			)
						
      */

    val rdd5:RDD[(String,Int)] = rdd4.reduceByKey((a,b) => a + b)
    
     /*
      rdd5 = RDD(
      task2
      ("hadoop",1)
      ("hbase",1)
      ("hive",1)
      ("java",4)
      
      task3=> 
      ("kafka",1)
      ("python",1)
      ("scala",1)
      ("spark",3)
      )
      */
    
    rdd5.foreach(println)
    
    /*
    val rdd7 = sc.textFile("file:/home/hduser/sparkdata/courses.txt")
   .map(line => line.split(" "))
   .flatMap(arr => arr)
   .map(word => (word,1))
   .reduceByKey((a,b) => a + b)
   .foreach(println)
   .sortBykey()
                 
    */
     
  }    
}

