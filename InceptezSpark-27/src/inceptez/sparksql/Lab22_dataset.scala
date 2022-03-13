package inceptez.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class customerds(custid:Int,fname:String,lname:String,age:Int,prof:String)

object Lab22_dataset {
  
  def main(args:Array[String])=
  {
    
    val spark = SparkSession.builder().appName("Lab22-dataset").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    
    import spark.implicits._
    
    val ds = df.as[customerds]
    //or
    
    val ds1 = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof").as[customerds]

    
    //filter prof = Pilot in dataframe - There is no compile type safety
    //df.filter("prof1111 =  'Pilot'").show()
    
    //Compile safety is there
    ds.filter(cust => cust.prof == "Pilot").show()
    
    //Dataset to Dataframe
    val df2 = ds.toDF()
    
    //DataFrame to Datset
    val ds2 = df.as[customerds]
    
    //Dataframe to RDD
    val rdd2 = df2.rdd
    
    //Dataset to RDD
    val rdd3 = ds.rdd
    
    val rdd4 = rdd3.map(x => (x.custid,x.prof))
    
    val rdd5 = rdd2.map(x => (x.getInt(0),x.getString(4)))
    
    val df4 = rdd3.toDF()
    
    //Dataframe = Dataset{Row}
    
    //RDD to Dataset
    val ds3 = spark.createDataset(rdd3)
    
    //RDD to Dataframe
    val df5 = rdd3.toDF()
    
    //RDD[Row] to Dataframe
    
    val custid = StructField("Custid",IntegerType,true)
    
    val fname = StructField("Fname",StringType,true)
    
    val lname = StructField("Lname",StringType,true)
    
    val age = StructField("age",IntegerType,true)
    
    val prof = StructField("Profession",StringType,true)
    
    val schema = StructType(List(custid,fname,lname,age,prof))

    val df6 = spark.createDataFrame(rdd2, schema) 
    
    val fncustdata = (cust:customerds) => 
      {
        if(cust.fname.startsWith("a") && cust.age > 50)
         true
        else
          false
                 
      }
      
      val ds4 = ds.filter(x => 
        {
          if(x.fname.startsWith("a") && x.age > 50)
            
            true
          else
            false
            
         })
        
      //or
      val df7 = ds.filter(fncustdata)
      //or
      val ds7 = ds.filter("fname like 'a%' and age > 50")
      //or
      ds.createTempView("tblcustomer")
      spark.sql("select * from tblcustomer where fname = 'a%' and age > 50 ").show()
  }
  
}