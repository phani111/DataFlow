from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("myApp").setMaster("local")
sc = SparkContext(conf=conf)
print(sc.parallelize([1, 3, 4]).count())

