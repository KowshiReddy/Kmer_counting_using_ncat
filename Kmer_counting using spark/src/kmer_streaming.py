#importing the required libraries
import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

# This is for my personal computer
os.environ['PYSPARK_PYTHON'] = r'C:/Users/reddy/AppData/Local/Programs/Python/Python310/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:/Users/reddy/AppData/Local/Programs/Python/Python310/python.exe'

# Defining the function of kmers 
def ge_kmers_ko(line, k=3):
    return [line[i:i+k] for i in range(len(line) - k + 1)]


if __name__ == "__main__":
    #setting the spark (initilization and the connection)
    conf = SparkConf().setAppName("KMerCount").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    #asking it to connect every 10 seconds.
    ssc = StreamingContext(sc, 10)  

    # reading the data by connecting it to the local host
    lines_ko = ssc.socketTextStream("localhost", 9999)

    # Generating the kmers by using the kmers fucntion
    kmers_ko = lines_ko.flatMap(lambda line: ge_kmers_ko(line, k=3))
    kmer_counts_ko = kmers_ko.map(lambda kmer: (kmer, 1)).reduceByKey(lambda x, y: x + y)
    kmer_counts_ko.pprint()
    #session start
    ssc.start()
    ssc.awaitTermination()
