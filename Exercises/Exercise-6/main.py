from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder\
        .appName('Exercise6') \
        .getOrCreate()
    data = spark.read.csv("/Users/mohan_sharma/Desktop/data engineer practice/exercise1/Exercises/Exercise-6/data")
    data.createOrReplaceTempView("data_tbl")
    spark.sql("select date(start_time),avg(tripduration)")


if __name__ == '__main__':
    main()
