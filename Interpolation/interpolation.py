import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedShuffleSplit
from category_encoders.target_encoder import TargetEncoder
import math
from interpolation import dataprocessing

"""
Purpose: Generate set of dwellings that we could potentially interpolate values for column by fill in
dwellings_df: dataframe with unique dwellings with known addresses/blocks
returns: dataframe with numb_between_real column, and only dwellings that fulfill criteria
"""
def same_next(dwellings_df, column = "BLOCK_NUM"):
    dwellings_df = dwellings_df.copy()
    dwellings_df[str(column) + "_next"] = dwellings_df[column].shift(-1)
    dwellings_df = dwellings_df[dwellings_df[str(column) + "_next"] == dwellings_df[column]]
    dwellings_df["num_between_real"] = dwellings_df["num_between"] - 1
    dwellings_df = dwellings_df[dwellings_df["num_between_real"] != 0]
    return dwellings_df

"""
Purpose: limit number of dwellings in between known values for fill in
df: dataframe with num_between_real column filled in for all dwellings
limit: 
"""
def limit_dwellings_between(df, limit):
    df["num_between_real"] =  df["num_between_real"].ffill()
    return df[df["num_between_real"] <= limit].copy()

"""
Purpose: Get dwellings with relevant columns for fill in
df: dataframe with all dwellings, with sequence information
column: column to seek consecutive value
returns: dataframes with relevant dwellings for fillin, and the values they would be filled in with
"""
def get_consecutive_dwellings(df, column = "BLOCK_NUM"):

    if column == "street_name":
        dataprocessing.create_street_house(df, "CD_ADDRESS")

    next_col = str(column) + "_next"
    dataframes = []
    find = None
    for row in df.itertuples():
        if row.Known == 1:
            index_start = row.Index
            find = getattr(row, next_col)

        elif find is not None and find == getattr(row, column):
            index_end = row.Index + 1
            dataframes.append(df.iloc[index_start:index_end])
            find = None

    return pd.concat(dataframes)

"""
Purpose: creates a centroid from given x, y points
x: x values
y: y values
"""
def make_centroid(x,y):
    return(sum(x)/len(x), sum(y)/len(y))

"""
Purpose: Analyze model prediction by class, in order to understand imabalanced class issue
df: dataframe containing both x and y data
model: a fitted model
x_col: columns the model was trained on
y_col: name of column with y data
"""
def class_analysis(df, y):
    classes = df[y].unique()
    df_success = df[df["predicted"] == df[y]]
    df_failure = df[df["predicted"] != df[y]]

    print("Success rate:", len(df_success)/len(df))
    print("Failure rate:", len(df_failure)/len(df))

    info = {"record count":{num:sum(df[y] == num) for num in classes},
    "actual proportion":{num:sum(df[y] == num)/len(df) for num in classes},
    "predicted proportion":{num:sum(df["predicted"] == num)/len(df) for num in classes},
    "predicted correctly":{num:div_zero(sum(df_success[y] == num),len(df_success)) for num in classes},
    "predicted incorrectly":{num:div_zero(sum(df_failure[y]==num),len(df_failure)) for num in classes},
    "proportion of class predicted correctly":{num:sum(df_success[y] == num)/sum(df[y] == num) for num in classes},
    "proportion of class predicted incorrectly":{num:sum(df_failure[y] == num)/sum(df[y] == num) for num in classes}}

    return pd.DataFrame(info).sort_index()


"""
Purpose: Create stratified train test data for census records so that dwellings are only in one part of the data
df: dataframe with census records of interest
dwelling_col: name of dwelling number column
y_col: name of column with y data
"""
def stratified_train_test(df, y, dwelling_col, stratified = True):

    if stratified:
        db = df.loc[:, [dwelling_col, y]].groupby(dwelling_col, as_index=False).first().reset_index(
            drop=True).copy()
        for train, test in StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=123).split(db[dwelling_col],
                                                                                                     db[y]):
            train_dwellings = db.loc[train, dwelling_col]
    else:
        dwellings = df[dwelling_col].unique()
        train_dwellings = np.random.choice(dwellings, round(len(dwellings)*0.75), replace = False)

    train = df[df[dwelling_col].isin(list(train_dwellings))].copy()
    test = df[~df[dwelling_col].isin(list(train_dwellings))].copy()
    return train,test

"""
Purpose: divsion, but returns 0 if denominator is 0
Used in class_analysis() to prevent errors in case of no successes or no failures
n: numerator
d: denominator
"""
def div_zero(n, d):
    return n / d if d else 0

"""
Purpose: Target encodes train and test data, subseting the train data by features, and using y as class labels
Train: Training dataset
Test: Test dataset
Features: list of column names to target encode
y: name of class label (what will be predicted)
"""
def target_encoder(train, test, features, y):
    targetencoder = TargetEncoder(cols = features).fit(train.loc[:,features], train[y])
    train_encoded = targetencoder.transform(train.loc[:,features], train[y])
    test_encoded = targetencoder.transform(test.loc[:,features])
    return (train_encoded, test_encoded, train[y], test[y])

