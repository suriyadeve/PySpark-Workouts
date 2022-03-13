package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object Lab08_df_api {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab08_df_api").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val dftxn = spark.read.format("csv")
            .option("delimiter",",")
            .option("inferschema",true)
            .load("file:/home/hduser/hive/data/txns")
            .toDF("txnid","txndata","custid","amount","categoty","product","city","state","paymenttype")
            
        
    dftxn.show()
    
    dftxn.printSchema()
    
    //select txnid.amount,product,city,state from txn where state = "Texas"
    
    //val dftxn1 = dftxn.filter(dftxn("state") === "Texas")
    
    //val dftxn1 = dftxn.filter(col("state" === "Texas"))
    
    import spark.implicits._
    
    //val dftxn1 = dftxn.filter($"state" === "Texas")
    
    val dftxn1 = dftxn.filter("state = 'Texas'")
    
    dftxn1.show
    
     val texastranscount = dftxn1.count()
     
     println(s"Total Transaction in Texas: $texastranscount")
     
     //val dftxn2 = dftxn.filter($"state" === "Texas" || $"state" === "California")
     
     //val dftxn2 = dftxn.filter($"state" === "Texas" && $"state" === "California")
     
     //val dftxn2 = dftxn.filter("state = 'Texas' or state = 'California'")
     
     
     //select txnid.amount,product,city,state from txn where paymenttype = 'cash' and (state = 'Texas' or state = 'California')
     
     val dftxn3 = dftxn.filter("paymenttype = 'cash' and (state = 'California')")
     
     ///where, its alias of filter
     
     val dftxn4 = dftxn.where("paymenttype = 'cash' and (state = 'Texas' or state = 'California')")
     
     //distinct states
     
     //select distinct state from txns
     
     dftxn.select("state").distinct().show()
     
     //select amount,product,city,state from txns
     
     dftxn.select("amount","product","city","state").show()
     
     val dftxn5 = dftxn.select("amount","product","city","state")
     
     dftxn5.printSchema()
     
     //dupilcates
     
     dftxn.select("city","state").show()
     
     //remove duplicates
     //select distinct city,state from txns where state = 'California' 
     
     dftxn.filter("state = 'California'").select("city","state").distinct().show()
     
     //select distinct city as txncity,state as txnstate from txns where state = 'California'
     
     dftxn.filter("state = 'California'").select(col("city").alias("txncity"),col("state").alias("txnstate")).distinct().show()
     
          



  }
  
}