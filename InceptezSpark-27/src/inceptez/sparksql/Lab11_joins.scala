package inceptez.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Lab11_joins {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab11_joins").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
        
    val student = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/sparkdata/student.csv").toDF("studid","studname")
    
    val mark = spark.read.format("csv").option("inferschema",true).option("delimiter",",").load("file:/home/hduser/sparkdata/marks.csv").toDF("studid","mark1","mark2","mark3")
    
    //select * from student inner join mark on student.studid = mark.studid 
    
    val studentmark = student.join(mark,student("studid") === mark("studid"), "inner")
    studentmark.show()
    
    //select student.studid,student.studname,mark.mark1,mark.mark2,mark.mark3 from student inner join mark on student.studid = mark.studid 
    
    studentmark.select(student("studid").alias("id"),mark("studid"),student("studname"),mark("mark1"),mark("mark2"),mark("mark3")).show()
    
    studentmark.select(col("*")).show()
    
    //select * from student left outer join mark on student.studid = mark.studid 
    
    student.join(mark,student("studid") === mark("studid"), "left_outer").show()
    
    //select * from student right outer join mark on student.studid = mark.studid 
    student.join(mark,student("studid") === mark("studid"),"right_outer").show()
    
    //select * from student full outer join mark on student.studid = mark.studid 
    student.join(mark, student("studid") === mark("studid"),"outer").show()

     
     //select * from student left semi join mark on student.studid = mark.studid 
     //list only left side table columns
     //list only the left side table rows that are matching with right side table rows
     
     //select * from student where studid in (select studid from mark)
    
    student.join(mark,student("studid") === mark("studid"),"left_semi")
    
    
    //list only left side table columns
     //list only the left side table rows that are not matching with right side table rows
     //select * from student left semi join mark on student.studid = mark.studid 
     
     //select * from student where studid not in (select studid from mark)
    
    student.join(mark,student("studid") === mark("studid"),"left_anti")
    
    //cross join
    student.crossJoin(mark).show()
    
  }
  
}