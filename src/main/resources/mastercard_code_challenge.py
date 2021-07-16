# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 11:55:39 2021

@author: bhanu
"""
# Import necessary modules and libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext

sqc = SQLContext(sc)

sparkApp = spark.builder.appName('MyApp').getOrCreate()


def df_calls(transaction, customer):
    """
    This function takes two csv files, create spark dataframes, cleans data, joins both dataframes and retuns multiple dataframes created in transformation process
    :param transaction: full file path including filename: string
    :param customer: full file path including filename: string
    :return: combined_df, customers_df, customers_wo_transaction, cust_transaction_with_cat_df: all spark dataframes
    """
    # read customer transaction
    cust_transaction_df = sparkApp.read.option('header', 'true').option('encoding', 'Windows-1252').csv(
        transaction)

    # Step 1 replace null with average
    averageAmount = cust_transaction_df.select(avg("amount")).collect()[0][0]
    cust_transaction_no_null_df = cust_transaction_df.na.fill(str(averageAmount), ["amount"])

    # Step2 remove unknown amount
    cust_transaction_no_unknown_df = cust_transaction_no_null_df.filter(cust_transaction_no_null_df.amount != 'unknown')

    # step 3 add year
    cust_transaction_with_year_df = cust_transaction_no_unknown_df.withColumn('Year', year(
        to_date(cust_transaction_no_unknown_df.transaction_date, 'dd-mm-yyyy')))

    # Step four add campaign type
    cust_transaction_with_ct_df = cust_transaction_with_year_df.withColumn("campaign_type", lit("retail marketing"))

    # Step 5 read customer data
    customers_df = sparkApp.read.option('header', 'true').option('encoding', 'Windows-1252').csv(
        customer)

    # step 6 customers without transaction
    customers_wo_transaction = customers_df.join(cust_transaction_with_ct_df,
                                                 (customers_df.cust_id == cust_transaction_with_ct_df.cust_id),
                                                 'left_anti')

    # Step 7 set null country values to unknown, combined customer and transactions
    combined_df = customers_df.join(cust_transaction_with_ct_df,
                                    (customers_df.cust_id == cust_transaction_with_ct_df.cust_id), 'right').fillna(
        'unknown', ['country'])

    # Step 8 add category
    cust_transaction_with_cat_df = combined_df.withColumn('category',
                                                          when(combined_df.amount.cast(IntegerType()) > 5000, 'High') \
                                                          .when(combined_df.amount.cast(IntegerType()).between(4000,
                                                                                                               5000),
                                                                'Medium') \
                                                          .when(combined_df.amount.cast(IntegerType()) < 4000, 'Low'))
    return combined_df, customers_df, customers_wo_transaction, cust_transaction_with_cat_df


def main():
    transaction = 'E:\\Bhanu Files\\Documents\\cust_transaction.csv'
    customer = 'E:\\Bhanu Files\\Documents\\customers.csv'
    combined_df, customers_df, customers_wo_transaction, cust_transaction_with_cat_df = df_calls(transaction, customer)
    print('Custers and transactions combined:\n')
    combined_df.show()
    # customers_df.show()
    print('Customers without transactions:\n')
    customers_wo_transaction.show()
    print('Customers transactions with categories:\n')
    cust_transaction_with_cat_df.show()

if __name__ == '__main__':
    main()
#