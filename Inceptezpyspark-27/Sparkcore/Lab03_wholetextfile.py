from pyspark import SparkContext


def main():
    sc = SparkContext(master="local",appName="Lab03")
    sc.setLogLevel("ERROR")
    
    tdata = sc.wholeTextFiles("file:/home/hduser/sparkdata/sparkdata1")
    
    print(tdata.count())
        
    tdata.foreach(lambda x: print(x[0]))
    
    tdata.foreach(lambda x: print(x[1]))
     
    print("============================1")
    
    tdata1 = tdata.map(lambda x: x[0])
    
    filenames = tdata1.collect()
    
    print(type(filenames))
    
    print(filenames)
    
    lst = [10,20,30,"Apple"]
    
    rdd1 = sc.parallelize(lst)
    
    lst1 = rdd1.collect()
    
    print(lst1)

main()
