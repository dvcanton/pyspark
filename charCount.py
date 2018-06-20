from pystark import SparkContext

textFile = "prevert.txt"

sc = SparkContext("local", "charCount")

# cache(): persist the RDD with the default storage level (MEMORY_ONLY)
textData = sc.textFile(textFile).cache()

numAs = textData.filter(lambda s: 'a' in s).count() # Counting As
numBs = textData.filter(lambda s: 'b' in s).count() # Counting Bs

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
