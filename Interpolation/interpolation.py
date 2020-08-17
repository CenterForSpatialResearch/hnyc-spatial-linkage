import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from haversine import haversine, Unit
from sklearn.model_selection import train_test_split

"""
Uses regex to generate a street name column and house number column from address, modifies the original dataframe
df: data frame
address: name of column where address listed as house number and street name
returns: None, dataframe is modified in place
"""
def create_street_house(df, address):
    df[["house_number", "street_name"]] = df[address].str.split(" ", 1, expand = True)
    df["house_number"] = df["house_number"].str.extract(r'(\d+)')#.astype('int64', errors = 'ignore') #deal with numbers when they are listed as 124-125 (choose the first one) there's also one that's 2A which becomes 2
    df["house_number"] = df["house_number"].apply(pd.to_numeric, errors='coerce').dropna()

"""
Generates columns needed to create sequence id based on distance, modifying original dataframe or used in groupby.apply
df: dataframe with unique dwellings
X: name of column with latitude
Y: name of column with longitude
Address: (optional) name of column with address, if included creates a column with the next address
Ward: (optional) name of column with ward number, if included creates a column with the next ward
returns: dataframe
"""
def col_for_seq(df, X = "CD_X", Y = "CD_Y"):
    df["dwelling_num_listed"] = df.index
    df["next_dnl"] = df["dwelling_num_listed"].shift(-1)
    df["next_x"] = df[X].shift(-1)
    df["next_y"] = df[Y].shift(-1)

    df['dist'] = df.apply(lambda row: haversine((row[X], row[Y]), (row["next_x"], row["next_y"]), unit=Unit.MILES), axis=1)
    df["num_between"] = df["next_dnl"] - df["dwelling_num_listed"] #really number between plus one
    return df

"""
Creates train and test data
data_1880: 1880 dataframe
data_1850: 1850 dataframe
cols: list of columns to include for training dataset
both: boolean, if True creates training data that combines 1880 and 1850 dataset
returns: list of train_X, train_Y, test_1880_X, test_1880_y, test_1850_X, test_1850_y
"""
def create_train_test_data(data_1880, data_1850, cols, y = "house_number", both = True):
    data_1880 = data_1880.dropna(subset = [y])
    data_1850 = data_1850.dropna(subset = [y])
    train_1880_X, test_1880_X, train_1880_y, test_1880_y = train_test_split(data_1880.loc[:,cols], data_1880.loc[:,y], random_state = 23)

    if not both:
        return [train_1880_X, train_1880_y, test_1880_X, test_1880_y, data_1850.loc[:,cols], data_1850.loc[:,y]]
    else:
        train_1850_X, test_1850_X, train_1850_y, test_1850_y = train_test_split(data_1850.loc[:, cols],
                                                                                data_1850.loc[:, y], random_state = 23)
        return [pd.concat([train_1880_X, train_1850_X]), pd.concat([train_1880_y, train_1850_y]), test_1880_X, test_1880_y, test_1850_X, test_1850_y]

"""
Creates sequence id based on distance between consecutive dwellings
df: dataframe containing unique dwellings
d: maximum distance between dwellings, anything larger than this means a sequence break
returns: dataframe
"""
def get_dist_seq(df, d):
    df = df.copy()
    df["sequence_id"] = np.where(df["dist"] > d, df["dist"].index, np.nan)
    df["sequence_id"].bfill(inplace = True)
    df["sequence_id"].fillna(df.tail(1).index[0], inplace = True)
    df["sequence_len"] = df.groupby("sequence_id")["num_between"].transform('sum')
    sequence_map = {ids:num for ids, num in zip(df["sequence_id"].unique(), range(len(df["sequence_id"].unique())))}
    df["sequence_order_enum"] = df.apply(lambda row: sequence_map[row["sequence_id"]], axis = 1)
    return df

"""
Tunes selection of maximum distance between known consecutive sequences, but minimizing the difference in sequence
sequences and then the number of sequences with length in min_len
dwellings_dropped: dataframe containing unique dwellings with columns created in col_for_seq, none of the col_for_seq
columns should have nans
dist_op: list of possible maximum distances
min_len: list of sequence lengths, function minimizes number of sequences that have those lengths (secondary to
difference in sequence length)
returns: dictionary with selected dist, and corresponding dataframe
"""
def tune_sequence_dist(dwellings_dropped, dist_op, min_len, ward_column = "CENSUS_WARD_NUM"):
    seq_len_diff_min = np.inf
    for op in dist_op:
        df = dwellings_dropped.groupby(ward_column, as_index = False).apply(lambda x: get_dist_seq(x, op))
        diff = max(df["sequence_len"]) - min(df["sequence_len"])

        #check if new op value creates less of a difference in sequence length variation
        if diff < seq_len_diff_min:
            seq_len_diff_min = max(df["sequence_len"]) - min(df["sequence_len"])
            num_min_len_seq = len(df[df["sequence_len"].isin(min_len)])
            dwelling_sequences = df
            d = op

        #if there is no difference in sequence length variation check for difference in number of sequences with length
        #in min length
        elif diff == seq_len_diff_min:
            if len(df[df["sequence_len"].isin(min_len)]) < num_min_len_seq:
                num_min_len_seq = len(df[df["sequence_len"].isin(min_len)])
                dwelling_sequences = df
                d = op

    return {"dist":d, "df":dwelling_sequences}

"""
graphs relative features importances on a horizontal bar chart
features: list of features in order
coefs: corresponding coefficients (relative importance)
title: Name of graph
returns None, but prints graph
"""
def graph_coefs(features, coefs, title):
    fig, ax = plt.subplots(1, figsize=(20, 10))
    ax.barh(features, coefs)
    ax.set_title(title)
    ax.set_xlabel("Coefficients")
    ax.set_ylabel("Features")
    plt.show()

"""
Takes whole data set and dwellings level data set and joins them
all: dataset with all entries
cols: list of column names to include from dwellings level dataset
joins: list of columns to perform the join on
return: dataframe of all census with dwellings info
"""
def dwellings_to_all(all, dwellings, cols, joins):
    df = all.merge(dwellings.loc[:,cols], how = "left", on = joins, validate = 'many_to_one')
    #df.dropna(subset = ["house_number"], inplace = True)
    return df

"""
This takes a dataframe of a single sequence and creates a sequence_order column, designed to be used with groupby("sequence_id").apply()
df: dataframe with a single sequence, including "num_between" column
returns: dataframe with sequence_order column
"""
def sequence_order(df):
    df["sequence_order"] = df["num_between"].cumsum()
    return df

"""
Takes the census dataframes and processes them to create dataframes ready to use for modeling
census_1850: dataframe with 1850 census information
census_1880: dataframe with 1880 census information
returns: dwellings and entry level dataframes for both 1880 and 1850
"""

def sequence_datasets(census_1850, census_1880):
    census_1850 = census_1850.dropna(subset=["CENSUS_DWELLING_NUM"]).copy()

    dwellings_1850 = census_1850.groupby(["WARD_NUM", "CENSUS_DWELLING_NUM"], as_index=False).first()
    dwellings_1850 = dwellings_1850.dropna(subset = ["CD_ADDRESS"]).copy()
    dwellings_1880 = census_1880.drop_duplicates(subset=["CENSUS_ADDRESS"]).reset_index(drop = True).copy()

    col_for_seq(dwellings_1850, "CD_X", "CD_Y")
    col_for_seq(dwellings_1880, "POINT_X", "POINT_Y")

    dwellings_1850 = get_dist_seq(dwellings_1850, 0.15)[2]
    dwellings_1880 = get_dist_seq(dwellings_1880, 0.15)[2]

    dwellings_1850 = dwellings_1850.groupby("sequence_id").apply(sequence_order)
    dwellings_1880 = dwellings_1880.groupby("sequence_id").apply(sequence_order)

    #Not super sure what's happening here, come back and check this
    dwellings_1850 = dwellings_1850.groupby(["WARD_NUM", "CENSUS_DWELLING_NUM"], as_index=False).first()
    dwellings_1880 = dwellings_1880.drop_duplicates(subset=["CENSUS_ADDRESS"]).reset_index(drop=True).copy()

    census_1880_model = dwellings_to_all(census_1880, dwellings_1880,
                                                       ["CENSUS_MATCH_ADDR", "sequence_id", "sequence_order", "num_between"], ["CENSUS_MATCH_ADDR"])
    census_1850_model = dwellings_to_all(census_1850, dwellings_1850,
                                                       ["WARD_NUM", "CENSUS_DWELLING_NUM", "sequence_id", "sequence_order", "num_between", "sequence_order_enum"],
                                                       ["WARD_NUM", "CENSUS_DWELLING_NUM"])

    create_street_house(dwellings_1880, "CENSUS_ADDRESS")
    create_street_house(dwellings_1850, "CD_ADDRESS")
    create_street_house(census_1880_model, "CENSUS_ADDRESS")
    create_street_house(census_1850_model, "CD_ADDRESS")

    return [dwellings_1850, dwellings_1880, census_1850_model, census_1880_model]

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
Purpose: Combine dwellings with addresses, with added sequences and all dwellings
all_dwellings: dataframe with all dwellings
known_dwellings: dataframe with known_dwellings
returns: dataframe with all dwellings, with sequence ids, order_enum, order
"""
def all_dwellings_sequenced(all_dwellings, known_dwellings, ward_column = "CENSUS_WARD_NUM", dwelling_column = "CENSUS_DWELLING_NUM"):
    known_dwellings["Known"] = 1
    prediction_data = dwellings_to_all(all_dwellings, known_dwellings, list(
        set(list(known_dwellings.columns)).difference(list(all_dwellings.columns))) + [ward_column,
                                                                                       dwelling_column],
                                       [ward_column, dwelling_column])

    prediction_data["sequence_id"] = prediction_data["sequence_id"].ffill()
    prediction_data["sequence_order_enum"] = prediction_data["sequence_order_enum"].ffill()
    prediction_data = prediction_data.groupby("sequence_id").apply(sequence_order)
    return prediction_data

"""
Purpose: Get dwellings with relevant columns for fill in
df: dataframe with all dwellings, with sequence information
column: column to seek consecutive value
returns: dataframes with relevant dwellings for fillin, and the values they would be filled in with
"""
def get_consecutive_dwellings(df, column = "BLOCK_NUM"):

    if column == "street_name":
        create_street_house(df, "CD_ADDRESS")

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
