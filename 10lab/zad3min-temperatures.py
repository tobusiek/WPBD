from pyspark import SparkConf, SparkContext
from collections import Counter


conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    temperature = float(fields[3])
    return temperature


lines = sc.textFile("/input/temp.csv")
parsedLines = lines.map(parseLine)
stationTemps = parsedLines.map(lambda x: (x, 1))
tempNum = stationTemps.reduceByKey(lambda a, b: a+b)
tempNum.saveAsTextFile('/input/output')
    