package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lab12_sql_query {
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab12_sql_query").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val custdf = spark.read.format("csv").option("delimiter", ",").option("inferSchema", true).load("file:/home/hduser/hive/data/custs").toDF("custid", "fname", "lname", "age", "prof")
        
     //select * from customer where prof = 'Pilot' and age > 50
     //custdf.filter("prof = 'pilot and age > 50'").show()
        
      custdf.createOrReplaceTempView("tblcustomer")
        
     /* val df = spark.sql("select * from tblcustomer where prof = 'Pilot' and age > 50")
      
      val df1 = df.select("custid","fname","age")
      
      df1.show()*/
      
      spark.sql("select * from tblcustomer where prof = 'Pilot' and age > 50").select("custid","fname","age").show()
      
    val student = spark.read.format("csv").option("inferschema", true).load("file:/home/hduser/sparkdata/student.csv").toDF("studid", "studname")

    val mark = spark.read.format("csv").option("inferschema", true).option("delimiter", "~").load("file:/home/hduser/sparkdata/mark.csv").toDF("studid", "mark1", "mark2", "mark3")
    
    student.createOrReplaceTempView("tblstudent")
    
    mark.createOrReplaceTempView("tblmark")
    
    spark.sql("select * from tblstudent s inner join tblmark m on s.studid = m.studid").show
    
    spark.sql("select prof, count(custid) as customercount from tblcustomer group by prof").show()
    
    
    
  }
  
}