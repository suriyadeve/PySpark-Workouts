from pyspark import SparkContext

def main():
    
    #sc = SparkContext(master = "local", appName = "Lab10")
    sc = SparkContext(appName = "lab10")
    sc.setLogLevel("ERROR")
    
    emp = sc.textFile("file:/home/hduser/sparkdata/emp.csv").map(lambda x : x.split(",")).map(lambda x : (x[2],(x[0],x[1],x[3])))
    
    emp.foreach(print)
    
    dept = sc.textFile("file:/home/hduser/sparkdata/dept.csv").map(lambda x : x.split(",")).map(lambda x : (x[0],x[1]))
        
    print("==============================================")
    
    dept.foreach(print)
    
    empdept = emp.leftOuterJoin(dept)
    
    print("==============================================join")
    
    empdept.foreach(print)
    
main()