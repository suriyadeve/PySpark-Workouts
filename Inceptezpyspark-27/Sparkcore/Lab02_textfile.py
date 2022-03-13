from pyspark import SparkContext

def main():
    
    sc = SparkContext(master="local",appName="Lab02")
    sc.setLogLevel("ERROR")
    
    #Read from one file
    fdata = sc.textFile("file:/home/hduser/sparkdata/employee.txt")
    fdata.foreach(print)
    
    print("========================1")
     
    #Read from multiple files
    fdata1 = sc.textFile("file:/home/hduser/sparkdata/employee.txt,file:/home/hduser/sparkdata/emp")
     
    fdata1.foreach(print)
    print("=========================2")
     
    #Read all the files from the folder
    fdata2 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1")
    fdata2.foreach(print)
     
    print("===========================3")
     
    #Read all the textfiles from the folder 
    fdata3 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1/*.txt")
    fdata3.foreach(print)
     
    print("=============================4")
     
    #Read  multiple folder
    fdata4 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1,file:/home/hduser/sparkdata/test/testdata")
    fdata4.foreach(print)
     
    print("=============================4.1")
    #Read  one all folder files and other one files 
    fdata4a = sc.textFile("file:/home/hduser/sparkdata/sparkdata1,file:/home/hduser/sparkdata/testdata")
    fdata4a.foreach(print)
      
    print("=============================5")
    #Read from hdfs filesystem
    fdata5 = sc.textFile("hdfs://localhost:54310/user/hduser/customerdata")
    fdata5.foreach(print)
     
    print("=============================6")
    #Read from hdfs and local filesystem 
    fdata6 = sc.textFile("hdfs://localhost:54310/user/hduser/customerdata,file:/home/hduser/sparkdata/sparkdata1")
    fdata6.foreach(print)
     
    print("============================7")
     
    #Read file from without URI
    fdata7 = sc.textFile("/user/hduser/customerdata")
    fdata7.foreach(print)
    
    print("=============================8")
     
     
    #Read multiple folder from hdfs 
    fdata8 = sc.textFile("hdfs://localhost:54310/user/hduser/sports")
    fdata8.foreach(print)

main()


