package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lab13_globaltemp {
  
  def main(args:Array[String])=
  {
    /*
   
	Catalog implementations
	There is two catalog implementations :

	in-memory to create in-memory tables only available in the Spark session,
	hive to create persistent tables using an external Hive Metastore.
	
   */
  
    val spark = SparkSession.builder().appName("Lab13_globaltemp").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    spark.conf.set("spark.sql.crossjoin.enabled","false")
    
    val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    df.createOrReplaceTempView("tblcustomer")
    
    spark.sql("select custid,fname from tblcutomer limit 10").show(false)
     
    df.createOrReplaceGlobalTempView("tblcustomer_global")
    
    spark.sql("select custid,fname from global_temp.tblcustomer_global limit 10").show()
    
    val spark1 = spark.newSession()
    
    spark.sql("select custid,fname from  global_temp.tblcustomer_global limit 10").show()
    
    spark.catalog.listTables("default").show()
    
    spark.catalog.listDatabases().show()
    
    spark1.catalog.listDatabases().show()
    
    
    
}
}