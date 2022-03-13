package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lab10_df_operations {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab10_df_operations").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val custdf = spark.read.format("csv").option("delimiter",",")
                .option("inferschema",true)
                .load("file:/home/hduser/hive/data/custs")
                .toDF("custid","fname","lname","age","prof")
    
    //Add column to the dataframe
    //select *, "USA" as country from customer 
   
    val df = custdf.withColumn("country", lit("USA"))
    
    //select *,concat(fname,'-',lname) as fullname, 'USA' as country from customer
    
    val df1 = df.withColumn("Fullname", concat(df("fname"),lit("-"),df("lname")))
    
    val df2 = df1.withColumn("seniorperson",df1("age") > 50)
    
    /*select *, case when age < 20 then 'Chi
                     when age < 40 then 'Young'
                     when age < 60 then 'Old'
     								else 'Very Old' end as category from customer
       */
    
    val df3 = df2.withColumn("category", when(df2("age") < 20, "Child")
              .when(df2("age") < 40, "Young")
              .when(df2("age") < 60,"Old")
              .otherwise("very Old"))
    
    
    
     //Generate sequence id
    
    val df4 = df3.withColumn("RowID", monotonically_increasing_id() +1)
    
    df4.select("fname","lname","age").show()
    
    df4.select(col("fname"),df4("lname"),df4("age")).show()
    
     //casting
    
    val df5 = df4.select(df4("age").cast("string"),df4("custid"))
    
    //Column Renaming 
    
    val df6 = df4.withColumnRenamed("category","Agecategory")
    
   //Drop column from dataframe
    val df7 = df6.drop("Agecategory")
    
    //Remove multiple columns
    df6.drop("age","category").printSchema()
    
    //Deal with null
    //select * from custs where prof is null
    
    df6.filter("prof is null").show()
    
    //or
    
    df6.filter(df6("prof").isNull).show()
    
    //Not null
    df6.filter("prof is not null").show()
    
    //or
    
    df6.filter(df6("prof").isNotNull).show()
        
    
    //Replace null value with 'Unknown'
    
    df6.na.fill("UnKnown").show()
    
    df6.filter("prof is null").na.fill("Unknown").show()
    
    //Replace integer columns null values 0 
    
    df6.na.fill(0).show()
    
    //specific column need to replace
    df6.na.fill("NA",Array("prof","fname")).show()
    
    //Drop rows with null values
    df6.na.drop().count()
    
    df6.describe("prof").show()
    
    //select *, case when country='India' then 'Malaysia' else country end from customer 
    
    
    
    
        
  }
}