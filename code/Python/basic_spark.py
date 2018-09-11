#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'zhangslob'
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python36'


from pyspark import SparkConf, SparkContext


# Initializing Spark
conf = SparkConf().setAppName('basic_spark').setMaster('local')
sc = SparkContext(conf=conf)

# Using the Shell
# $ ./bin/pyspark --master local[4]
# $ ./bin/pyspark --master local[4] --py-files code.py

# Resilient Distributed Datasets (RDDs)
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print(distData.reduce(lambda a, b: a + b))  # 15


# External Datasets
distFile = sc.textFile('data.txt')
print(distFile.map(lambda s: len(s)))
# distFile.map(lambda s: len(s)).filter(lambda x : x > 210).count()
# distFile.filter(lambda s: len(s)>500).collect()


# Saving and Loading SequenceFiles
# you must save as tuple
rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd.saveAsSequenceFile('rdd')
print(sorted(sc.sequenceFile('rdd').collect()))


# RDD Operations
lines = sc.textFile("data.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)

lineLengths.persist()  # use it later

