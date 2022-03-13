package inceptez.sparksql

import org.apache.spark.sql.SparkSession

object Lab17_jdbc_operations {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab17-jdbc").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //jdbcconurl,username,password,table
    val df = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","(select * from customer) tblc")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()
    
    df.printSchema()
    
    df.show()
    
    df.select("custid","city","age").show()
    
    df.createOrReplaceTempView("tblcustomer")
    
    spark.sql("select * from tblcustomer where transactamt > 10000").show()
    
    //spark-shell --jars file:/home/hduser/install/mysql-connector-java.jar
    
    //spark-submit --class inceptez.sparksql.Lab17_jdbc_operations --master local[*] --jars file:/home/hduser/install/mysql-connector-java.jar sparkdata/sparkworkouts.jar
    
    
    
    
  }
  
}