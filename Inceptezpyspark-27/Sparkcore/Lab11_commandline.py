from pyspark import SparkContext
import sys


def main():
    """
    21-01-2022
    
    spark-submit 
    --master local 
    --class incetez.sparkcore.Lab11_commandline file:/home/hduser/sparkworkouts.jar hdfs://localhost:54310/user/hduser/coursefees.txt 2
    """
    
    if(len(sys.argv) > 2):
        
        filepath = sys.argv[1]
        partitions = int(sys.argv[2])
        
        
        #sc  = SparkContext(master = "local",appName = "Lab11")
        sc = SparkContext()
        rdd = sc.textFile(filepath,partitions)
        rdd.foreach(print)
         
    
    else:
    
        print("Invali Input")
    
main()