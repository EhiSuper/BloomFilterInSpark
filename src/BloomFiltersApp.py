from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import BloomFiltersParameters as bfp
import BloomFiltersConstruction as bfc
import FalsePositiveRateTest as fpr

conf = SparkConf().setAppName('BloomFiltersConstruction').setMaster('local')
sc = SparkContext(conf=conf)

file_path = "Data/data.tsv"
false_positive_ratio = 0.05

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv(file_path, sep=r'\t',header=True).select('tconst', 'averageRating')
rdd = df.rdd

bloom_parameters = bfp.getBloomFiltersParameters(rdd, false_positive_ratio)

bloom_filters = bfc.getBloomFilters(rdd, bloom_parameters)

false_positive_rates = fpr.getFalsePositiveRates(rdd, bloom_filters, bloom_parameters)
print(false_positive_rates)
sc.stop() # stop the SparkContext