from pystark import SparkContext
from operator import add

sc = SparkContext("local", "charCount")

nums = sc.parallelize([1,2,3,4,5])
sum = nums.reduce(add)

print("Adding all elements results in %i" % sum)
