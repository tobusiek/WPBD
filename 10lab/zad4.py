from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def split_to_words(line):
    return line.split()

text = sc.textFile("/input/globse.en")
words = text.flatMap(split_to_words)
words_tuples = words.map(lambda x: (x, 1))
counts = words_tuples.reduceByKey(lambda a,b: a+b)
counts.saveAsTextFile("zad4output")
