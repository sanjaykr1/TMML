import os
import unittest
from pandas.testing import assert_frame_equal

from Accounts.account_data import AccountData
from Accounts.customer_data import CustomerData
from Accounts.tasks import Pipeline
from Accounts.test.parent import PySparkTest
from Accounts.utility import Utility


class MyTestCase(PySparkTest):

    def test_account_data(self):
        """
        Test account data class by creating custom dataframe and comparing it with
        account_data object dataframe
        :return:
        """
        ac_obj = AccountData("test_account_data.csv")
        cols = ["customerId", "accountId", "balance"]
        data = [("INA001", "ACC001", "1000"),
                ("INA002", "ACC002", "2003"),
                ("INA003", "ACC003", "992"),
                ("INA001", "ACC004", "1002"),
                ("INA005", "ACC005", "1999"),
                ("INA003", "ACC006", "288"),
                ("INA006", "ACC007", "9929"),
                ]
        test_df = super().spark.createDataFrame(data, cols)
        super().dataframe_equal(ac_obj.df, test_df, "customerId")

    def test_customer_data(self):
        """
        Test customer data class by creating custom dataframe and comparing it with
        customer_data object dataframe
        :return:
        """
        cust_obj = CustomerData("test_customer_data.csv")
        cols = ["customerId", "forename", "surname"]
        data = [("INA001", "Jack", "Wills"),
                ("INA002", "Martin", "Sueol"),
                ("INA003", "Sean", "Paul"),
                ("INA005", "Daniel", "Kenny"),
                ("INA006", "Vikram", "Kumar"),
                ]
        test_df = super().spark.createDataFrame(data, cols)
        super().dataframe_equal(cust_obj.df, test_df, "customerId")

    def test_utility_read_data(self):
        u1 = Utility()
        df = u1.readfile("test_customer_data.csv")
        self.assertTrue(df.head())

    def test_utility_write_data(self):
        cols = ["customerId", "forename", "surname"]
        data = [("INA001", "Jack", "Wills"),
                ("INA002", "Martin", "Sueol"),
                ("INA003", "Sean", "Paul")]
        test_df = super().spark.createDataFrame(data, cols)
        Utility.writefile(self, test_df, "utility_write_data")
        self.assertTrue(os.path.isdir("C:\\Users\\B00827\\IdeaProjects\\TMML\\Accounts\\test\\utility_write_data"))

    def test_associated_acc(self):
        u1 = Utility()
        more_than_2 = u1.readfile("more_than_two_acc.csv")
        associated_acc = u1.readfile("associated_test.csv")
        top_5 = u1.readfile("top_5_acc.csv")
        pipeline = Pipeline("test_customer_data.csv", "test_account_data.csv")
        pipeline.associated_acc()
        test_more_than_2 = u1.readfile("more_than_2_acc")
        test_associated = u1.readfile("total_accounts")
        test_top_5 = u1.readfile("top5_acc")

        super().dataframe_equal(more_than_2, test_more_than_2, "customerId")
        super().dataframe_equal(associated_acc, test_associated, "customerId")
        super().dataframe_equal(top_5, test_top_5, "customerId")
