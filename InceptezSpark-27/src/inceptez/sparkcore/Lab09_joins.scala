package inceptez.sparkcore

import org.apache.spark.SparkContext

/*
Given filename student.csv and marks.csv

student.csv
1,Lokesh
2,Bhupesh
3,Amit
4,Ratan
5,Dinesh

marks.csv
1~90~80~95
2~88~90~89
3~78~76~70
4~92~69~89
5~88~70~86


Accomplish the followings:-

1. Load the files into RDD and get the studentid,studentname,mark1,mark2,marks3,totalmarks
2. Get the highest and lowest totalmarks scored  

tblstudent
-studid
-studname

tblmark
-studid
-mark1
-mark2
-mark3

//Highest Mark
select * from (
select s.studid,s.name,m.mark1,m.mark2,m.mark3,(m.mark1 + m.mark2 + m.mark3) as totalmarks 
from tblstudent s join tblmarks m on s.studid = m.studid ) A order by totalmarks desc limit 1

//Lowest Mark
select * from (
select s.studid,s.name,m.mark1,m.mark2,m.mark3,(m.mark1 + m.mark2 + m.mark3) as totalmarks 
from tblstudent s join tblmarks m on s.studid = m.studid ) A order by totalmarks limit 1


 */

object Lab09_joins {
  
 def main(args: Array[String])=
 {
   
   /*
Given filename student.csv and marks.csv

student.csv
1,Lokesh
2,Bhupesh
3,Amit
4,Ratan
5,Dinesh

marks.csv
1~90~80~95
2~88~90~89
3~78~76~70
4~92~69~89
5~88~70~86


Accomplish the followings:-

1. Load the files into RDD and get the studentid,studentname,mark1,mark2,marks3,totalmarks
2. Get the highest and lowest totalmarks scored  

tblstudent
-studid
-studname

tblmark
-studid
-mark1
-mark2
-mark3

//Highest Mark
select * from (
select s.studid,s.name,m.mark1,m.mark2,m.mark3,(m.mark1 + m.mark2 + m.mark3) as totalmarks 
from tblstudent s join tblmarks m on s.studid = m.studid ) A order by totalmarks desc limit 1

//Lowest Mark
select * from (
select s.studid,s.name,m.mark1,m.mark2,m.mark3,(m.mark1 + m.mark2 + m.mark3) as totalmarks 
from tblstudent s join tblmarks m on s.studid = m.studid ) A order by totalmarks limit 1


 */
   //val sc = new SparkContext(master = "local",appName="lab09")
   val sc = new SparkContext()
   
   val student = sc.textFile("file:/home/hduser/sparkdata/student.csv")
   
   val student1 = student.map(line => line.split(","))
   
   val student2 = student1.map(line => (line(0),line(1)))
   
   student.foreach(println)
   
   println("============================01")
   
   student2.foreach(println)
   
   val mark = sc.textFile("file:/home/hduser/sparkdata/marks.csv")
   
   val mark1 = mark.map(line => line.split("~"))
   
   val mark2 = mark1.map(line => (line(0),(line(1),line(2),line(3))))
   
   println("==========markdata============")
   
   mark.foreach(println)
   
   println("===============================02")
   
   mark2.foreach(println)
   
   val studentmark = student2.join(mark2,4)
   
   println("=========join===================")
   
   studentmark.foreach(println)
   
   //(_1,_2)
      //(1,(Lokesh,(90,80,95)))
      
      //_1 = 1
      //_2 = (Lokesh,(90,80,95))
      
      
      //_2._1 = Lokesh
      
      //_2._2 = (90,80,95)
      
      //_2._2._1 = 90
      
      //_2._2._2 = 80
      
      //_2._2._3 = 95
   
   
   val studentmark1 = studentmark.map(data => (data._1,data._2._1,data._2._2._1.toInt,data._2._2._2.toInt,data._2._2._3.toInt))
   
   val studentmark2 = studentmark1.map(s => (s._1, s._2, s._3, s._4, s._5, s._3 + s._4 + s._5))
   
   println("================================03")
   
   studentmark2.foreach(println)
   
   val studentmarksort = studentmark2.sortBy(x => x._6,false,1)
   
   val maxstudent = studentmarksort.first()

   println(s"Maximum Mark scored Student Info: $maxstudent")
   
     
   val studentmarksort1 = studentmark2.sortBy(x => x._6,true,1)
   
   val minstudent = studentmarksort1.first()
   
   
   println(s"Minimum Mark Scores student Info $minstudent")
   
   
 }
  
  
}