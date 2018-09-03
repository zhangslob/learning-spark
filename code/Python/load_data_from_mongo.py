#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'zhangslob'

import os
from pyspark.sql import SparkSession

# set PYSPARK_PYTHON to python36
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python36'

# load mongo data
input_uri = "mongodb://127.0.0.1:spark.spark_test"
output_uri = "mongodb://127.0.0.1:spark.spark_test"

my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0')\
    .getOrCreate()

df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()



