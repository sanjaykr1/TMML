import logging
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from sales.base import Utility

logger = logging.getLogger(__name__)


class Sales(Utility):

    def __init__(self, filename, delimit, schema):
        """
        Constructor of sales class will create dataframe based on given parameters
        :param filename: file to be read
        :param delimit: delimiter in the file
        :param schema: custom schema
        """
        super().__init__()
        # super().spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.filename = filename
        self.delimit = delimit
        self.schema = schema
        self.df = super().readfile(self.filename, self.delimit, self.schema)

    def tot_revenue(self):
        """
        Method to calculate total revenue per region
        :return: None
        """
        df_region = self.df.groupby("Region").agg(f.sum("Total Revenue").alias("Total Revenue_per_region"))
        df_region.show(truncate=False)
        super().writefile(df_region, "region_revenue")

    def household_units(self):
        """
        Method to evaluate total household units sold.
        :return: None
        """
        df_household = self.df.filter(f.col("Item Type") == "Household").sort("Units Sold", ascending=False)
        df_household = df_household.select("Country", "Units Sold").limit(5)
        df_household.show()
        super().writefile(df_household, "household_units")

    def tot_profit(self):
        """
        Method to calculate total profit between 2011 and 2015 for Asia region
        :return:
        """
        df_asia = self.df.filter((f.col("Region") == "Asia") &
                                 ((f.col("Order Date") >= '2011-01-01') & (f.col("Order Date") <= '2015-12-31')))
        tot_profit = df_asia.agg(f.sum(f.col("Total Profit"))).collect()[0][0]
        print("Total profit in Asia region: ", tot_profit)
        df_asia = df_asia.withColumn("Total_profit_Asia", when(f.col("Region") == "Asia", tot_profit)).\
            orderBy("Order Date")
        df_asia.show()
        super().writefile(df_asia, "total_profit")


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

delim = ";"
file = "dataset/salesData.csv"
sale = Sales(file, delim, sales_schema)
sale.tot_revenue()
sale.household_units()
sale.tot_profit()
