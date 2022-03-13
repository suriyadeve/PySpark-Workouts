package inceptez.sparksql

import org.apache.spark.sql.SparkSession


object Lab26_hive_write {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab26-hive").master("local").config("hive.metastore.uris","thrift://localhost:9083").enableHiveSupport().getOrCreate()
  //val spark = SparkSession.builder().appName("Lab26-hive").master("local").config("hive.metastore.uris","thrift://localhost:9083").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferSchema",true).load("file:/home/hduser/hive/data/txns").toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
    
   //default - parquet
    //written the dataframe data into hive table 
    //df.write.saveAsTable("default.tbltrans")
   
    
    
    //The name of ORC implementation. It can be one of native and hive. native means the native ORC support. hive means the ORC library in Hive.
    
    spark.conf.set("spark.sql.orc.impl","hive")
    
    
    //When set to false, Spark SQL will use the Hive SerDe for ORC tables instead of the built in support.
    
    
    spark.conf.set("spark.sql.hive.convertMetastoreOrc",false)
    
    //orc
    //written the dataframe data into hive table
    
    df.write.format("orc").mode("overwrite").saveAsTable("default.tbltrans_orc")
    println("Data written into hive")
    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

  
}