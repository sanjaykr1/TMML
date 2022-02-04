from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s : %(levelname)s :%(name)s :%(message)s',
    datefmt="%m/%d/%Y %I:%H:%S %p",
    filename="logfile.log",
    filemode="w",
    level=logging.INFO
)


class Utility:

    def __init__(self):
        logger.info("Creating spark session from utility class")
        self.spark = SparkSession.builder.appName("POC1").master("local").getOrCreate()
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    def readfile(self, filename, delimit=",", schema=None):
        """
        read a csv file in a dataframe
        :param delimit: custom delimiter can be provided
        :param filename: file which needs to be read
        :param schema: custom schema for the file, none by default
        :return: df
        """
        logger.info("Reading %s file into dataframe", filename)
        try:
            if schema:
                df = self.spark.read.\
                    option("delimiter", delimit).\
                    csv(filename, header=True, schema=schema)
                df = self.date_col_convert(df)
                # df = df.withColumn("Order Date", f.date_format("Order Date", "dd/MM/yyyy"))
            else:
                df = self.spark.read. \
                    option("delimiter", delimit). \
                    csv(filename, header=True, inferSchema=True)
                df = self.date_col_convert(df)
                # df = df.withColumn("Order Date", f.date_format("Order Date", "dd/MM/yyyy"))
        except Exception as e:
            logger.exception("Unable to read file Exception %s occurred", e)
            print("Unable to save file due to exception %s. ", e)
        else:
            return df

    def writefile(self, df, filename):
        """
        Write dataframe to csv file with the given filename
        :param df: dataframe to be written
        :param filename: filename in which dataframe is written
        :return: None
        """
        logger.info("Writing dataframe to file %s", filename)
        try:
            df.write.mode("overwrite").csv(filename)
        except Exception as e:
            logger.exception("Unable to save file Exception %s occurred", e)
            print("Unable to save file due to", e)

    def date_col_convert(self, df):
        """
        Convert date column from string to date type
        :return:
        """
        df = df.withColumn("Order Date", f.regexp_replace("Order Date", "-", "/"))
        df = df.withColumn("Order Date", f.to_date("Order Date", "dd/mm/yyyy"))
        return df
