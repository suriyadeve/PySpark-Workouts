package inceptez.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import utils.commons

object Lab23_udf {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab23-udf").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    val getdis = udf(commons.getdiscount)
    
    df.withColumn("discount",getdis(df("age"))).show()
    
    val fnfullname = udf(commons.getfullname _)
    
    df.withColumn("discount",getdis(df("age"))).withColumn("fullname",fnfullname(df("fname"),df("lname"))).show()
  
    val fnisprime = udf(isprime)
    
    df.withColumn("IsPrime",fnisprime(df("age"))).show()
    
    df.select(df("age"),fnisprime(df("age")).alias("IsPrime")).show()
    
    df.createTempView("tblcust")
    
    //If udf function need to use in sql then we need to register
    
    spark.udf.register("checkprime",isprime)
    
    spark.sql("select age,checkprime(age) from tblcust").show()
    
  }
  
  val isprime = (a:Int) =>
    {
      var isprime = true
      for(i <- 2 to a-1)
      {
        if(a % i == 0)
        isprime = false
      }
      isprime
        
    }
       
    
  
}