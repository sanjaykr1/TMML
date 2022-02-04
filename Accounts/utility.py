from pyspark.sql import SparkSession
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
    """
    Utility class for implementing common methods being used by other classes
    """
    def __init__(self):
        self.spark = SparkSession.builder.appName("POC1").master("local").getOrCreate()
        logging.info("Creating spark session from Utility class")

    def readfile(self, filename):
        """
        Reads csv file into a dataframe and returns the dataframe
        :param filename: .csv file which needs to be read
        :return: df
        """
        logger.info("Reading csv file with filename %s", filename)
        df = self.spark.read.csv(filename, header=True)
        return df

    def writefile(self, df, filename):
        """
        Write dataframe to csv file with the given filename
        :param df: dataframe to be written
        :param filename: filename in which dataframe is written
        :return: None
        """
        df.write.mode("overwrite").csv(filename)
