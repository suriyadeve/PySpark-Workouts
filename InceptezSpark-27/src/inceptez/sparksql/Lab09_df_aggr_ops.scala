package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col,sum,min,max,count,round,desc,asc}



object Lab09_df_aggr_ops {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab09_df_agr_ops").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val dftxn = spark.read.format("csv")
            .option("delimiter",",")
            .option("inferSchema",true)
            .load("file:/home/hduser/hive/data/txns")
            .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
            
      
           
   //select state, max("amount") from txn group by state
     
    val df1 = dftxn.groupBy("state").max("amount").show()
    
    //select state,city, max("amount") from txn group by state,city
    
    val df2 = dftxn.groupBy("state","city").max("amount").show()
    //val df2 = dftxn.filter("state === 'California'").groupBy("state","city").max("amount").show()
    
    //select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
    //count("txnid") from txn group by state, city    
    
    dftxn.groupBy("state","city")
    .agg(max("amount").alias("maxsalesamount"),
        min("amount").alias("minsalesamount"),
        sum("amount").alias("totalsales"),
        count("txnid").alias("transcount"))
    .show()
    
    //select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
    //count("txnid") from txn group by state, city order by state desc,city asc
  
  dftxn.groupBy("state","city")
        .agg(max("amount").alias("maxsalesamount"),
            min("amount").alias("minsalesamount"),
            sum("amount").alias("totalsales"),
            count("txnid").alias("transcount"))
        .orderBy(desc("state"),asc("city"))
        .show()
  
  //select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
    //count("txnid") from txn group by state, city order by state,city
        
   dftxn.groupBy("state","city")
         .agg(max("amount").alias("maxsalesamount"),
              min("amount").alias("minsalesamount"),
              round(sum("amount"),2).alias("totalsalesamount"),
              count("txnid").alias("txncount"))
          .orderBy("state","city")
          .show()
   
   //select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
   //count("txnid") from txn group by state, city having count("txnid") < 500 order by state,city
          
   dftxn.groupBy("state","city")
        .agg(max("amount").alias("maxamount"),
             min("amount").alias("minamount"),
             round(sum("amount"),2).alias("tatalsalesamount"),
             count("txnid").alias("txncount"))
       .orderBy("state","city")
       .filter("txncount <500")
       .show()
   
        
          
  
  }
  
}