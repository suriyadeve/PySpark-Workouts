package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Lab14_window_function {
  
  def main(args:Array[String])=
    
  {
    val spark = SparkSession.builder().appName("Lab14_window_Function").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/sparkdata/empdatanew.txt").toDF("empid","dept","salary")
    
    df.createOrReplaceTempView("tblemp")
    
    spark.sql("select empid,dept,salary,row_number() over (PARTITION BY dept ORDER BY salary desc) as row_num from tblemp").show()
    
    val winspec = Window.partitionBy("dept").orderBy(desc("salary"))
    
    val df3 = df.withColumn("row_num",row_number().over(winspec))
    
    df3.show()
    
    val df4 = df3.withColumn("rank",rank().over(winspec))
    
    val winspec1 = Window.partitionBy("dept").orderBy(desc("salary"))
    
    df3.withColumn("denserank",dense_rank().over(winspec1)).show()
    
    df.withColumn("ntile",ntile(2).over(winspec1)).show()

        
    
    
    
  }
  
}