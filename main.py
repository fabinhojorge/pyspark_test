#!/usr/bin/env python3

""" Project to test the Spark code """

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# from pyspark.sql.types import ArrayType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType,\
#     StructField, StructType, BooleanType


def load_csv(_spark, file_name=''):
    file_name = file_name if file_name != '' else "./datasets/AB_NYC_2019.csv"
    _df = _spark.read.option("header", "true").option("sep", ",").csv(file_name)
    return _df


def cast_column(_df, _column, _type='int'):
    df_modified = _df.withColumn(_column, _df[_column].cast(_type))
    return df_modified


def filter_column_by_type(_df, _column, _type='int'):
    df_modified = _df.filter(_df[_column].cast(_type).isNotNull())
    return df_modified


def filter_and_cast_by_type(_df, _column, _type='int'):
    df_modified = filter_column_by_type(_df, _column, _type)
    df_modified = cast_column(df_modified, _column, _type)
    return df_modified


def apply_filter_and_cast(_df):
    _df = filter_and_cast_by_type(_df, 'id', 'int')
    _df = filter_and_cast_by_type(_df, 'host_id', 'int')
    _df = filter_and_cast_by_type(_df, 'minimum_nights', 'int')
    _df = filter_and_cast_by_type(_df, 'number_of_reviews', 'int')
    _df = filter_and_cast_by_type(_df, 'calculated_host_listings_count', 'int')
    _df = filter_and_cast_by_type(_df, 'availability_365', 'int')
    _df = filter_and_cast_by_type(_df, 'price', 'float')
    _df = filter_and_cast_by_type(_df, 'reviews_per_month', 'float')
    _df = _df.withColumn('last_review', F.to_date(_df.last_review, format='yyyy-MM-dd'))
    return _df


def execute_main(spark):
    _df = load_csv(spark)
    _df = apply_filter_and_cast(_df)
    _df.printSchema()
    return _df


# if __name__ == '__main__':
sc = SparkSession.builder.appName("app_test").getOrCreate()
df = execute_main(sc)
# df.write.parquet('nyc_taxi_parquet')
# sc.stop()
