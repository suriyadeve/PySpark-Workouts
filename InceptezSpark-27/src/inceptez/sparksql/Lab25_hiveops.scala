package inceptez.sparksql
import org.apache.spark.sql.SparkSession

object Lab25_hiveops {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab25-hiveops").master("local").config("hive.metastore.uris","thrift://localhost:9083").enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        spark.sql("show databases").show()
        
        val df = spark.sql("select * from default.tblbook")
                
        df.show()
        
        //create table in hive using hql
        spark.sql("create table default.tblmarks(studid int, mark1 int,mark2 int,mark3 int) row format delimited fields terminated by ','")
        
        spark.sql("load data local inpath '/home/hduser/sparkdata/mark.csv' into table tblmarks")
        
        println("Table created and data loaded into the hive table")
        
        
  }
  
}