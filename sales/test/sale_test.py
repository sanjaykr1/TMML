import os
from pyspark.sql import functions as f
from sales.base import Utility
from sales.sale import Sales
from sales.test.parent import PySparkTest


class SalesTest(PySparkTest):

    @classmethod
    def setUpClass(cls):
        cls.f1 = "test_sales.csv"
        cls.schema = super().sales_schema
        cls.delim = ";"
        cls.s = Sales(cls.f1, cls.delim, cls.schema)

    def test_utility_read_data(self):
        u1 = Utility()
        df = u1.readfile("test_sales.csv")
        self.assertTrue(df.head())

    def test_utility_write_data(self):
        cols = ["col1", "col2", "col3"]
        data = [("1", "Val1", "Abc1"),
                ("2", "Val2", "Abc2"),
                ("3", "Val3", "Abc3")]
        test_df = super().spark.createDataFrame(data, cols)
        Utility.writefile(self, test_df, "test_base_write")
        self.assertTrue(os.path.isdir("C:\\Users\\B00827\\IdeaProjects\\TMML\\Accounts\\test\\utility_write_data"))

    def test_tot_revenue(self):
        self.s.tot_revenue()
        test_revenue = Utility.readfile(self, "test_region_revenue.csv")
        revenue = Utility.readfile(self, "region_revenue")
        super().dataframe_equal(test_revenue, revenue, "Region")

    def test_household(self):
        self.s.household_units()
        test_household = Utility.readfile(self, "household_test.csv")
        household = Utility.readfile(self, "household_units")
        super().dataframe_equal(test_household, household, "Country")

    def test_total_profit(self):
        self.s.tot_profit()
        test_profit = Utility.readfile(self, "test_total_profit.csv")
        profit = Utility.readfile(self, "total_profit")
        super().dataframe_equal(test_profit, profit, "Country")
