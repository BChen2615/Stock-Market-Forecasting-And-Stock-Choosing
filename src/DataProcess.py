# This is a python file for data processing
import pandas as pd
import numpy as np

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