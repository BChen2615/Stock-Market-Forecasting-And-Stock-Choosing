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
    if interval > return_data.groupby("Stock_ID").size().max():
        a = return_data.groupby("Stock_ID").size().max()
        return_data[f"MA_{interval}"] = return_data.groupby("Stock_ID")["Close"].transform(lambda x: x.rolling(a).mean())

        return return_data

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

    original_df = df.copy()

    #-- Filtering data to where the date in range of pred_date - 365 days to pred_date - 1 day --#
    original_df = filter_for_date(original_df, start_date= pred_date - dt.timedelta(365),
                                  end_date= pred_date - dt.timedelta(1))
    for i in [5, 10, 20, 60, 120, 240]:
        original_df = average_price(i, original_df)

    #-- Calculating the features --#

    #-- Calculating the percentage change between last day and second last day --#
    # Name of this calculation will have _suffix of _delta_pct

    sid = original_df["Stock_ID"].unique()
    return_df = pd.DataFrame()
    return_df["Stock_ID"] = sid
    transition_df = original_df.sort_values('Date', ascending= False).groupby('Stock_ID', as_index= False).head(2)
    transition_df = transition_df.sort_values('Date')
    new_transition_df = transition_df.groupby('Stock_ID', as_index= False)[["Close", "MA_5", "MA_10", "MA_20",
                                                                            "MA_60", "MA_120", "MA_240"]].transform("pct_change")
    transition_df = transition_df.join(new_transition_df, rsuffix="_delta_pct")
    transition_df = transition_df.dropna(subset= ["Close_delta_pct"])
    return_df = return_df.merge(transition_df, on= "Stock_ID", how= "right")

    #-- Calculating the percentage change between second last day and third last day --#
    # Name of this calculation will have _suffix of _delta_pct_2

    transition_df = original_df.sort_values('Date', ascending= False).groupby('Stock_ID', as_index= False).nth[1:3]
    transition_df = transition_df.sort_values('Date')
    new_transition_df = transition_df.groupby('Stock_ID', as_index= False)[["Close", "MA_5", "MA_10", "MA_20",
                                                                            "MA_60", "MA_120", "MA_240"]].transform("pct_change")
    transition_df = transition_df.join(new_transition_df, rsuffix="_delta_pct_2")
    transition_df = transition_df.dropna(subset= ["Close_delta_pct_2"])
    transition_df = transition_df[["Stock_ID", "Close_delta_pct_2", "MA_5_delta_pct_2", "MA_10_delta_pct_2",
                                    "MA_20_delta_pct_2", "MA_60_delta_pct_2", "MA_120_delta_pct_2", "MA_240_delta_pct_2"]]
    return_df = return_df.merge(transition_df, on= "Stock_ID", how= "right")

    #-- Calculating the volume change between last day and second last day --#

    # The name of this calculation will have _suffix of _delta

    # This will not show in the final df that return, since this is just the help data
    # for calculating the acceleration

    transition_df = original_df.sort_values('Date', ascending= False).groupby('Stock_ID', as_index= False).head(2)
    transition_df = transition_df.sort_values('Date')
    new_transition_df = transition_df.groupby('Stock_ID', as_index= False)[["Volume"]].transform("diff")
    transition_df = transition_df.join(new_transition_df, rsuffix="_delta")
    transition_df = transition_df.dropna(subset= ["Volume_delta"])
    transition_df = transition_df[["Stock_ID", "Volume_delta"]]
    return_df = return_df.merge(transition_df, on= "Stock_ID", how= "right")
    
    #-- Calculating the acceleration of MA's and Close --#
    # The name of this calculation will have _suffix of _acc
    
    ma = ["MA_5", "MA_10", "MA_20", "MA_60", "MA_120", "MA_240", "Close"]

    first_ma = ["MA_5_delta_pct", "MA_10_delta_pct", "MA_20_delta_pct",
                 "MA_60_delta_pct", "MA_120_delta_pct", "MA_240_delta_pct", "Close_delta_pct"]

    second_ma = ["MA_5_delta_pct_2", "MA_10_delta_pct_2", "MA_20_delta_pct_2",
                 "MA_60_delta_pct_2", "MA_120_delta_pct_2", "MA_240_delta_pct_2", "Close_delta_pct_2"]

    for i in range(5):
        return_df[f"{ma[i]}_acc"] = return_df[first_ma[i]] - return_df[second_ma[i]]

    return_df = return_df.drop(labels= second_ma, axis= 1)


    return return_df

    # TODO: The delta between second last day and last day in the dataframe, IDEA is to use group


    


