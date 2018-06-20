import random
from pyspark import SparkContext

NUM_SAMPLES = 100000000

sc = SparkContext("local", "piCalc")
# SparkContext params
# master: URL of the cluster it connects to
# appName: job name
# sparkHome: Spark installation directory
# pyFiles: .zip or .py files to send to the cluster and add to the PYTHONPATH
# environment: worker nodes env variables
# batchsize: (1 disable batching, 0 automatically choose the batch size)
# serializer: RDD serializer
# conf = object of L{SparkConf} to set all the Spark properties
# gateway: use an existing gateway and JVM, otherwise initializing a new JVM
# jsc: JavaSparkContext instance
# profiler_cls: class of custom Profiler used to do profiling (the default is pyspark.profiler.BasicProfiler).

def inside(p):
 x, y = random.random(), random.random()
 return x*x + y*y < 1

count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
pi = 4 * count / NUM_SAMPLES

print(“Pi is roughly”, pi)
