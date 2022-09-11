# Databricks notebook source
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import Row
import datetime

# COMMAND ----------


courses = [
    {
        'course_id': 1,
        'course_title': 'Mastering Python',
    },
    {
        'course_id': 2,
        'course_title': 'Data Engineering Essentials',
    },
    {
        'course_id': 3,
        'course_title': 'Mastering Pyspark',
    },
    {
        'course_id': 4,
        'course_title': 'AWS Essentials',
    },
    {
        'course_id': 5,
        'course_title': 'Docker 101',
    }
]

courses_df = spark.createDataFrame([Row(**course) for course in courses])


current_courses = [
    {
        'course_id': 1,
        'course_title': 'Mastering Python',
    },
    {
        'course_id': 2,
        'course_title': 'Data Engineering Essentials',
    },
    {
        'course_id': 3,
        'course_title': 'Mastering Pyspark',
    },
    {
        'course_id': 5,
        'course_title': 'Docker 102',
    }
    ,
    {
        'course_id': 6,
        'course_title': 'DP 100',
    }
]

current = spark.createDataFrame([Row(**current_course) for current_course in current_courses])
