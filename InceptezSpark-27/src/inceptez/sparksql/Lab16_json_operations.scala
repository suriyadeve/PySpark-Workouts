package inceptez.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Lab16_json_operations {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab16_json_operations").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val empschema =StructType(List(StructField("address",StructType(List(StructField("city",StringType,true),StructField("state",StringType,true), StructField("zipcode",StringType,true))),true), 
        StructField("designation",StringType,true),StructField("empid",LongType,true),
        StructField("empname",StringType,true), StructField("skills",ArrayType(StringType,true),true)))

    
    val df = spark.read.schema(empschema).format("json").load("file:/home/hduser/sparkdata/empdetails.json")
    
    df.printSchema()
    
    df.select(df("empid"),df("empname"),df("designation")).show()
    
    //skills - Array
    df.select(df("skills")(0),df("skills")(1)).show()
    
    //address - Struct
    df.select(df("address.zipcode"),df("address.city"),df("address.state")).show()
    
    

    
    df.createOrReplaceTempView("tblemployee")
    
    spark.sql("select empid,empname,desination,skills[0],skills[1],skills[2],address.city,address.state,address.state,address,zipcode from tblemployee").show()
    
    //convert array into a row
    val df1 = df.select(df("empid"),df("empname"),df("designation"),explode(df("skills")).alias("skilname"))
    
    df1.show()
    
    spark.sql("select empid,empname,designation,explode(skills) as skillsexpold,address.state,address.city,address.zipcode from tblemployee").show()
  
    val df2 = spark.read.schema(empschema).format("json").option("multiline",true).load("file:/home/hduser/sparkdata/empdetails1.json")
    /*
      There are 3 typical read modes and the default read mode is permissive.
		
    		permissive — All fields are set to null and corrupted records are placed in a string column called _corrupt_record.
    		dropMalformed — Drops all rows containing corrupt records.
    		failFast — Fails when corrupt records are encountered.
      */
    
    spark.read.format("json").option("mode","permissive").load("file:/home/hduser/sparkdata/empdetails.json").show()
    
    spark.read.format("json").option("mode","dropMalformed").load("file:/home/hduser/sparkdata/empdetails.json").show()
    
    spark.read.format("json").option("mode","failFast").load("file:/home/hduser/sparkdata/empdetails.json").show()
    
  }
  
}