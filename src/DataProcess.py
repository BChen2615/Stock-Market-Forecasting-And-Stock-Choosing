# This is a python file for data processing
import pandas as pd
import numpy as np
import datetime as dt

def average_price(interval, data):
    """
    Calculate the average price over a specified interval.

    Parameters:
    interval (int): The number of periods to calculate the average over, interval in days.
                    expected values are 5, 10, 20, 60, 120, 240.
    data (pd.DataFrame): The data containing price information with a 'Close' column.

    Returns:
    pd.DataFrame: A dataframe containing the average prices and the original data frame that input.
    """
    return_data = data.sort_values(by= ['Stock_ID', 'Date'])
    return_data[f"MA_{interval}"] = return_data.groupby("Stock_ID")["Close"].transform(lambda x: x.rolling(interval).mean())

    return return_data

def filter_for_date(df, start_date= dt.date(2020, 12, 24), end_date= dt.date.today()):
    """
    This is a function to filter the data based on the start date and end date

    :param df: (pd.dataframe) Should be a dataframe of a database with all the stocks and historical data
    :param start_date: (date) default is the date of 2020-12-24
    :param end_date: (date) default is the current date (Note that the current date may not include in the database
    :return: (pd.dataframe) filtered dataframe
    """
    filtered_data = df[
        (df["Date"] >= pd.to_datetime(start_date)) &
        (df["Date"] <= pd.to_datetime(end_date))
    ]
    return filtered_data

def prediction_data_processing(df, pred_date):
    """
    This is the function to process the data from database to the features for prediction

    :param df: (pd.dataframe) The dataframe that comes from the database
    :param pred_date: (date) The date that we want to predict the stock price for
    :return: (pd.dataframe) new dataframe that with each row is the one stock of taiwan stock market, and
            each column is the feature of the stock
    """

    return_df = df.copy()
    return_df = filter_for_date(return_df, pred_date - dt.timedelta(day= 365))


