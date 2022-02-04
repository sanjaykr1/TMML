import logging

from Accounts.customer_data import CustomerData
from Accounts.account_data import AccountData
from pyspark.sql import functions as f
from pyspark.sql import Window

logger = logging.getLogger(__name__)


class Pipeline(CustomerData, AccountData):
    """
    Pipeline class to perform aggregations
    """

    def __init__(self, customer, account):
        """
        Read customer and account data in dataframe
        :param customer: customer data filename
        :param account: account data filename
        """
        logger.info("Creating customer and account objects from Pipeline class.")
        self.cust_obj = CustomerData(customer)
        self.acc_obj = AccountData(account)

    def associated_acc(self):
        """
        Count total accounts for each customer
        tot_acc: dataframe with total accounts column for a customer
        cust_df2: dataframe with customer having more than 2 accounts
        top_5_acc: dataframe with top 5 customers having highest balance
        :return: None
        """
        logger.info("Finding associated accounts.")
        temp_df = self.cust_obj.df.join(self.acc_obj.df,
                                        (self.cust_obj.df.customerId == self.acc_obj.df.customerId), how='full').\
            select(self.cust_obj.df["*"], self.acc_obj.df["accountId"], self.acc_obj.df["balance"])
        w = Window.partitionBy("customerId")
        # tot_acc = temp_df.withColumn("No_of_Accounts", f.count("customerId").over(w))
        """
        Assigning no_of_account as 0 if accountId is null, otherwise count customerId.
        """
        logger.info("Counting number of accounts for each customer.")
        tot_acc = temp_df.withColumn("No_of_Accounts", f.when(f.col("accountId").isNull(), 0).
                                     otherwise(f.count("customerId").over(w)))
        logger.info("Filtering customers with more than 2 accounts.")
        cust_df2 = tot_acc.filter(f.col("No_of_Accounts") >= 2)
        cust_df2 = cust_df2.drop("accountId", "balance").dropDuplicates()

        tot_acc = tot_acc.dropDuplicates()
        logger.info("Sorting out top 5 accounts having highest balance.")
        top_5_acc = tot_acc.sort("balance", ascending=False).limit(5)

        super().writefile(tot_acc, "total_accounts")
        super().writefile(cust_df2, "more_than_2_acc")
        super().writefile(top_5_acc, "top5_acc")
        tot_acc.show()
        cust_df2.show()
        top_5_acc.show()


acc = "dataset/account_data.csv"
cust = "dataset/customer_data.csv"
obj1 = Pipeline(cust, acc)
obj1.associated_acc()
