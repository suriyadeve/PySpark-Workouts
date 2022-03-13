package inceptez.sparksql

import org.apache.spark.sql.SparkSession

object Lab18_jdbc_write {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab18_jdbc-write").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //Read from csv file
    val custdf = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")

   /* //Load data into mysql
    custdf.write.format("jdbc")
         .mode("overwrite")
         .option("url","jdbc:mysql://localhost/custdb")
         .option("user","root")
         .option("password","Root123$")
         .option("dbtable","tblcustomer")
         .option("driver","com.mysql.cj.jdbc.Driver")
         .save()
         
        println("Data successfully written into mysql database")*/
        
        //Load data into postgress
        custdf.write.format("jdbc")
        .mode("overwrite")
        .option("url","jdbc:postgresql://localhost/retail")
        .option("user","hduser")
        .option("password","hduser")
        .option("dbtable","tblcustomer")
        .option("driver","org.postgresql.Driver")
        .save()
        
        println("Data successfully written into postgress databases")
        
        //spark-shell --jars file:/home/hduser/install/mysql-connector-java.jar,file:/home/hduser/Downloads/postgresql-42.3.2.jar
        
   
  }
  
}