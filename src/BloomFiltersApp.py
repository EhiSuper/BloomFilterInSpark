from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import BloomFiltersParameters as bfp
import BloomFiltersConstruction as bfc
import FalsePositiveRateTest as fpr
import time
import sys

if len(sys.argv) != 4:
        print("Usage: driver <file> <false_positive rate> <partitions>",
              file=sys.stderr)
        sys.exit(-1)

start_time = time.time() # time for evaluations

#checking parameters
file_name = sys.argv[1]
try:
    file = open(f'./../Data/Input/{file_name}')
    file.close()
except:
    print("Sorry we cant find the file. Plese try again.")
    sys.exit(-1)

false_positive_rate = sys.argv[2]
try:
    false_positive_rate = float(false_positive_rate)
except:
    print("Sorry the desired false positive rate is not a float. Plase try again.")
    sys.exit(-1)
partitions = sys.argv[3]
try:
    partitions = int(partitions)
except:
    print("Sorry the number of partitions is not an int. Please try again.")
    sys.exit(-1)

conf = SparkConf().setAppName('BloomFiltersConstruction').setMaster(f'local[{partitions}]')
sc = SparkContext(conf=conf)

file_path = f'./../Data/Input/{file_name}'

spark = SparkSession.builder.getOrCreate()

for i in range(10):
    df = spark.read.csv(file_path, sep=r'\t',header=True).select('tconst', 'averageRating') # read the .tsv file and return a dataframe
    rdd = df.rdd

    bloom_parameters = bfp.getBloomFiltersParameters(rdd, false_positive_rate, i)
    broadcast_bloom_parameters = sc.broadcast(bloom_parameters)

    bloom_filters = bfc.getBloomFilters(rdd, broadcast_bloom_parameters, i)
    broadcast_bloom_filters = sc.broadcast(bloom_filters)

    false_positive_rates = fpr.getFalsePositiveRates(rdd, broadcast_bloom_filters, broadcast_bloom_parameters, i)
    print(false_positive_rates)

print((time.time()-start_time)*1000)
    
sc.stop() # stop the SparkContext
# 1 run 8 partitions no broadcast rdd in function: 37915
# 10 run 8 partitions no broadcast rdd in function: 284716
# 10 run 8 partitions broadcast parameters and filters rdd in function: 283007
# 10 run 8 partitions broadcast parameters and filters rdd cache: 267258