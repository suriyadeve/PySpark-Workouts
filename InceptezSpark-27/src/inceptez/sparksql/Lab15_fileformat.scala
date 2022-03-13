package inceptez.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object Lab15_fileformat {
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab15_fileformat").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    spark.conf.set("spark.sql.shuffle.partition",2)
    val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    //select prof,count(custid) from tblcustomer group by prof
    
    val df1 = df.groupBy("prof").count()
    df.createOrReplaceTempView("tblcustomer")
    
    val df2 = spark.sql("select prof,count(custid) as customercount from tblcustomer group by prof")
    
    //No shuffling - 1 partition
    df.write.format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    
    //Due to shuffling, default number of partition is 200
    df2.write.format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    
    //write as single partition
    df2.coalesce(1).write.format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    
     //Append,Overwrite,ErrorIfexist,Ignore 
    
    //overwrite
    
    df2.coalesce(1).write.mode("overwrite").format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    //Or
    df2.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    
    //append
    df2.coalesce(1).write.mode("append").format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    //Or
    df2.coalesce(1).write.mode(SaveMode.Append).format("csv").save("file:/home/hduser/sparkdata/write/customerbyprof")
    
   val df3 = df2.coalesce(1)
   
   //csv,json,parquet(default),orc
    
   df3.write.mode("append").format("json").save("file:/home/hduser/sparkdata/write/customerbyprofjson")
   
   df3.write.mode("append").format("orc").save("file:/home/hduser/sparkdata/write/customerbyproforc")
   
   df3.write.mode("append").format("parquet").save("file:/home/hduser/sparkdata/write/customerbyprofparquet")
   
   //read different file format  
   val jsondf = spark.read.format("json").load("file:/home/hduser/sparkdata/write/customerbyprofjson")
   jsondf.show()
   
   val orcdf = spark.read.format("orc").load("file:/home/hduser/sparkdata/write/customerbyproforc")
   
   val parquetdf = spark.read.format("parquet").load("file:/home/hduser/sparkdata/write/customerbyprofparquet")
    
  }
  
}