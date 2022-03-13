package inceptez.sparksql
import org.apache.spark.sql.SparkSession


object Lab19_etl_process {
  def main(args:Array[String])=
  {
    println("********************* Start Retail data ETL Process *****************")
    val spark = SparkSession.builder().appName("Lab19-jdbc").master("local").getOrCreate()
    
    println("Step 1: Read customer data from postgres")
    val dfcustomer = readcustomerdata(spark)
    
    println("Step 2: Read transaction data from mysql")
    val dftrans = readtransdata(spark)
    
    println("Step 3: Register dataframe as view for both dataframes as tbltxn and tblcust")
    dfcustomer.createOrReplaceTempView("tblcust")
    
    dftrans.createOrReplaceTempView("tbltxn")
    
    println("Step 4: Join 2 dataset by writing join query based on custid column")
    val dfdata = spark.sql("select state,count(txnid) as totaltrans from tbltxn t join tblcust c on t.custid = c.custid where c.prof = 'Pilot' group by state")
  
    println("Step 5: Write the output in local filesystem as json format")
    dfdata.write.format("json").mode("overwrite").save("file:/home/hduser/pilotstatedata")
    
    println("********************* Completed Retail data ETL Process *****************")
    
  }
  
  
  def readcustomerdata(spark:SparkSession)=
  {
    val df = spark.read.format("jdbc")
    .option("url","jdbc:postgresql://localhost/retail")
    .option("user","hduser")
    .option("password","hduser")
    .option("dbtable","tblcustomer")
    .option("driver","org.postgresql.Driver").load();
    
    df
    
  }
  
  def readtransdata(spark:SparkSession)=
  {
    val df = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","tbltrans")
    .option("driver","com.mysql.cj.jdbc.Driver").load();

    df
        
        
  }
  
}