from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Word Count").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data1 = sc.textFile('/home/ashok/IdeaProjects/PySparkExamples/PythonShellCommands/words.txt')
data2 = data1.flatMap(lambda x: x.split(' '))
data3 = data2.map(lambda x: (x, 1))
data4 = data3.reduceByKey(lambda x, y: x + y)
result = data4.collect()

for (word, count) in result:
    print("%s -> %i" % (word, count))

sc.stop()
