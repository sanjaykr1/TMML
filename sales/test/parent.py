import unittest
import logging
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class PySparkTest(unittest.TestCase):

    sales_schema = StructType([
        StructField("Region", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Item Type", StringType(), True),
        StructField("Sales Channel", StringType(), True),
        StructField("Order Priority", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Order ID", LongType(), True),
        StructField("Units Sold", DoubleType(), True),
        StructField("Unit Price", DoubleType(), True),
        StructField("Total Revenue", DoubleType(), True),
        StructField("Total Profit", DoubleType(), True),
    ])
    spark = SparkSession.builder.master('local').appName('testing').getOrCreate()

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master('local')
                .appName('testing')
                .getOrCreate())

    @classmethod
    def logger_initializer(cls):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

    @classmethod
    def setUpClass(cls):
        cls.logger_initializer()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @classmethod
    def dataframe_equal(cls, df1, df2, columns):
        df1 = df1.toPandas()
        df1 = df1.sort_values(by=columns).reset_index(drop=True)
        df2 = df2.toPandas()
        df2 = df2.sort_values(by=columns).reset_index(drop=True)
        assert_frame_equal(df1, df2)
