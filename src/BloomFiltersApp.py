from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import BloomFiltersParameters as Parameters
import BloomFiltersConstruction as Contruction
import FalsePositiveRateTest as Test
import time
import sys

if len(sys.argv) != 3:
    print("Usage: driver <file> <false_positive rate>",
          file=sys.stderr)
    sys.exit(-1)

start_time = time.time()  # time for evaluations

# checking parameters
file_name = sys.argv[1]
try:
    file = open(f'./../Data/Input/{file_name}')
    file.close()
except OSError:
    print("Sorry we cant find the file. Please try again.")
    sys.exit(-1)

false_positive_rate = sys.argv[2]
try:
    false_positive_rate = float(false_positive_rate)
except ValueError:
    print("Sorry the desired false positive rate is not a float. Please try again.")
    sys.exit(-1)

conf = SparkConf().setAppName('BloomFiltersConstruction')
sc = SparkContext(conf=conf)

file_path = f'./../Data/Input/{file_name}'

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(file_path, sep=r'\t', header=True).select("tconst",
                                                              'averageRating')  # read the .tsv file and return a
# dataframe
rdd = df.rdd

bloom_parameters = Parameters.get_bloom_filters_parameters(rdd, false_positive_rate)
broadcast_bloom_parameters = sc.broadcast(bloom_parameters)

bloom_filters = Contruction.get_bloom_filters(rdd, broadcast_bloom_parameters)
broadcast_bloom_filters = sc.broadcast(bloom_filters)

false_positive_rates = Test.get_false_positive_rates(rdd, broadcast_bloom_filters, broadcast_bloom_parameters)
print(false_positive_rates)

print((time.time() - start_time) * 1000)

sc.stop()  # stop the SparkContext
