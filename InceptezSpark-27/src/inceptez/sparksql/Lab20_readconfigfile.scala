package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.io.FileInputStream

object Lab20_readconfigfile {
 val prop = new Properties()
  
  def main(args:Array[String])=
  {
        
    val spark = SparkSession.builder().appName("Lab19-jdbc").master("local").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
     println("********************* Start Retail data ETL Process *****************")
     
     
    println("Read JDBC info from config.properties")
    val fs = new FileInputStream("/home/hduser/workspacespark/InceptezSpark-27/config.properties")
    prop.load(fs)
     
     println("Step 1: Read customer data from postgres")
     val dfcustomer = readcustomerdata(spark)
     
     println("Step 2: Read transaction data from mysql")
     val dftrans = readtransdata(spark)
     
     println("Step 3: Register dataframe as view for both dataframes as tbltxn and tblcust")
     dfcustomer.createOrReplaceTempView("tblcust")
     
     dftrans.createOrReplaceTempView("tbltxn")
     
     println("Step 4: Join 2 dataset by writing join query based on custid column")
     val dfdata = spark.sql("select state, count(txnid) as `total trans` from tbltxn t join tblcust c on t.custid = c.custid where c.prof = 'Pilot' group by state")
     
     
     println("Step 5: Write the output in local filesystem as json format")
     dfdata.coalesce(1).write.format("json").mode("overwrite").save("file:/home/hduser/pilotstatedata")
     
     println("********************* Completed Retail data ETL Process *****************")
     
  }
  
  def readcustomerdata(spark:SparkSession)=
  {
      val df = spark.read.format("jdbc")
      .option("url",prop.getProperty("pg_jdbcurl"))
      .option("user",prop.getProperty("pg_username"))
      .option("password",prop.getProperty("pg_password"))
      .option("dbtable",prop.getProperty("pg_table"))
      .option("driver",prop.getProperty("pg_driver")).load();
      
     df
  }
   
  def readtransdata(spark:SparkSession)=
  {
    val df = spark.read.format("jdbc")
     .option("url",prop.getProperty("mysql_jdbcurl"))
     .option("user",prop.getProperty("mysql_username"))
     .option("password",prop.getProperty("mysql_password"))
     .option("dbtable",prop.getProperty("mysql_table"))
     .option("driver",prop.getProperty("mysql_driver")).load();
    
     df
     
  }
}