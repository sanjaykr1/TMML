import unittest
import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pandas.testing import assert_frame_equal


class PySparkTest(unittest.TestCase):
    schema = StructType([
        StructField('Account_No', StringType(), True),
        StructField('DATE', DateType(), True),
        StructField('TRANSACTION_DETAILS', StringType(), True),
        StructField('CHQ_NO', StringType(), True),
        StructField('VALUE_DATE', DateType(), True),
        StructField('WITHDRAWAL_AMT', DecimalType(18, 2), True),
        StructField('DEPOSIT_AMT', DecimalType(18, 2), True),
        StructField('TransactionType', StringType(), True),
        StructField('TransactionAmount', DecimalType(18, 2), True),
        StructField('BALANCE_AMT', DecimalType(18, 2), True),
    ])
    expected_schema = StructType([
        StructField('CustomerName', StringType(), True),
        StructField('Account_No', StringType(), True),
        StructField('DATE', DateType(), True),
        StructField('TRANSACTION_DETAILS', StringType(), True),
        StructField('CHQ_NO', StringType(), True),
        StructField('VALUE_DATE', DateType(), True),
        StructField('WITHDRAWAL_AMT', DecimalType(18, 2), True),
        StructField('DEPOSIT_AMT', DecimalType(18, 2), True),
        StructField('TransactionType', StringType(), True),
        StructField('TransactionAmount', DecimalType(18, 2), True),
        StructField('BALANCE_AMT', DecimalType(18, 2), True),
        StructField("Month", StringType(), True),
        StructField("SumDeposit_PerMonth", DecimalType(18, 2), True),
        StructField("SumWithdrawal_PerMonth", DecimalType(18, 2), True),
        StructField("EndBalance_PerMonth", DecimalType(18, 2), True)
    ])
    config_parser = configparser.ConfigParser()
    config_file_path = r"C:\\Users\\ACER NITRO 5\\IdeaProjects\\Project1\\POC2_prog\\poc2.conf"
    config_parser.read(config_file_path)

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
