package inceptez.sparkcore

import org.apache.spark.SparkContext

object Lab02_textfile {
  def main(args:Array[String])=
  {
    val sc = new SparkContext(master="local",appName="Lab02")
    sc.setLogLevel("ERROR")
    
    //Read from one file
    val fdata = sc.textFile("file:/home/hduser/sparkdata/employee.txt")
    fdata.foreach(println)
    
    println("========================1")
    
    //Read from multiple files
    val fdata1 = sc.textFile("file:/home/hduser/sparkdata/employee.txt,file:/home/hduser/sparkdata/emp")
    
    fdata1.foreach(println)
    println("=========================2")
    
    //Read all the files from the folder
    val fdata2 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1")
    fdata2.foreach(println)
    
    println("===========================3")
    
    //Read all the textfiles from the folder 
    val fdata3 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1/*.txt")
    fdata3.foreach(println)
    
    println("=============================4")
    
    //Read  multiple folder
    val fdata4 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1,file:/home/hduser/sparkdata/test/testdata")
    fdata4.foreach(println)
    
     println("=============================4.1")
    //Read  one all folder files and other one files 
    val fdata4a = sc.textFile("file:/home/hduser/sparkdata/sparkdata1,file:/home/hduser/sparkdata/testdata")
    fdata4a.foreach(println)
     
    println("=============================5")
    //Read from hdfs filesystem
    val fdata5 = sc.textFile("hdfs://localhost:54310/user/hduser/customerdata")
    fdata5.foreach(println)
    
    println("=============================6")
    //Read from hdfs and local filesystem 
    val fdata6 = sc.textFile("hdfs://localhost:54310/user/hduser/customerdata,file:/home/hduser/sparkdata/sparkdata1")
    fdata6.foreach(println)
    
    println("============================7")
    
   //Read file from without URI
   val fdata7 = sc.textFile("/home/hduser/sparkdata/sparkdata1")
   fdata7.foreach(println)
   
    println("=============================8")
    
    
    //Read multiple folder from hdfs 
    val fdata8 = sc.textFile("hdfs://localhost:54310/user/hduser/sports")
    fdata8.foreach(println)
 
   
    
    
   
  }
}