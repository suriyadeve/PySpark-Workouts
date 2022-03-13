package inceptez.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._


object Lab24_datetime_ops 
{
  def main(args:Array[String])=
  {
      val spark = SparkSession.builder().appName("Lab24-date-ops").master("local").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
            
      val df = spark.read.format("csv").option("inferSchema",true).load("file:/home/hduser/hive/data/txns").toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
      
      //Default format - yyyy-MM-dd
      
      //Add current date
      
      val df1 = df.withColumn("load_dt",current_date())
      
      df1.show()
      
      //Add current date and time
      //Format - yyyy-MM-dd hh:mm:ss.mmm
      
      val df2 = df1.withColumn("load_ts", current_timestamp())
      
      df2.select("load_dt","load_ts").show()
      
      val df3 = df2.withColumn("load_unixts",unix_timestamp())
      df3.select("load_dt","load_ts","load_unixts").show()
      
      df.withColumn("load_dt", current_date()).withColumn("load_ts", current_timestamp()).withColumn("load_unixts", unix_timestamp()).select("load_dt","load_ts","load_unixts").show()
      
      //to_date = Convert string to date
      val df4 = df3.withColumn("txndate1", to_date(df3("txndate"),"MM-dd-yyyy"))
      df4.select("txndate","txndate1").show()
      
      //date_format = convert date from one format to another format in return in string type
      val df5 = df4.withColumn("txndate2",date_format(df4("txndate1"),"dd/MM/yyyy"))
      df5.select("txndate","txndate1","txndate2").show()
      
      //datediff = difference betweeen two dates 
      val df6 = df5.withColumn("diffindays", datediff(current_date(),df5("txndate1")))
      df6.select("txndate","txndate1","txndate2","diffindays").show()
      
      //months_between = no of months betweeen two dates 
      val df7 = df6.withColumn("noofmonths",round(months_between(current_date(),df6("txndate1"))))
      df7.select("txndate","txndate1","txndate2","diffindays","noofmonths").show()
      
     //Extract date,month and year
      df7.select(dayofmonth(df7("txndate")).as("day"),month(df7("txndate1")).as("month"),year(df7("txndate1")).as("year")).show()
      
      df7.select(df7("txndate1"),date_add(df7("txndate1"),2).alias("added_date"),date_sub(df7("txndate1"),2).as("subtract_date"),add_months(df7("txndate1"),2).as("add_month")).show()
      
      //year, month, month, dayofweek, dayofmonth, dayofyear, next_day, weekofyear 
      
      df7.select(col("txndate1"),year(col("txndate1")).as("year"),month(col("txndate1")).as("month"),dayofweek(col("txndate1")).as("dayofweek"),dayofmonth(col("txndate1")).as("dayofmonth"),dayofyear(col("txndate1")).as("dayofyear"),next_day(col("txndate1"),"Sunday").as("next_day"),weekofyear(col("txndate1")).as("weekofyear")).show()
         
     //to_timestamp() = convert string into timestamp format
      
      df7.select(to_timestamp(col("txndate"),"MM-dd-yyyy").alias("txndate_ts")).show()
          
      //unix_timestamp() = convert string into unix timestamp format
      df7.select(to_timestamp(col("txndate"),"MM-dd-yyyy").alias("txndate_ts"),unix_timestamp(col("txndate"),"MM-dd-yyyy").as("txndate_unixts")).show()
      
      val df8 = df7.withColumn("txndate_unix_ts",unix_timestamp(col("txndate"),"MM-dd-yyyy"))
   
      //from_unixtime = convert unixtimestamp into any required format and returns in string format
      df8.withColumn("tnxdate-fd", from_unixtime(df8("txndate_unix_ts"),"dd-MM-yyyy hh:mm")).select("tnxdate-fd").show()
      
      df8.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd-MM-yyyy HH:mm:ss").as("txndate-1")).show()
      
      df8.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd-MMM-yyyy HH:mm:ss").as("txndate-1")).show()
      
      df8.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd-MMM-yyyy").as("txndate-1")).show()
      
      df8.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd/MM/yyyy HH:mm:ss").as("txndate-1")).show()
      
      df.createOrReplaceTempView("tbltrans")
      
      spark.sql("select current_date() as load_dt,current_timestamp() as load_ts,unix_timestamp() as load_unixts from tbltrans").show()
      
      spark.sql("select txndate, to_date(txndate,'MM-dd-yyyy') as txndate1 from tbltrans").show()
      
      spark.sql("select txndate, date_format(current_date(),'dd-MM-yyyy') as txndate1 from tbltrans").show()
      
      
  }
  
  
}