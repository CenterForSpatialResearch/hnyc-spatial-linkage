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
Generates columns needed to create sequence id based on distance, modifying original dataframe
df: dataframe with unique dwellings
X: name of column with latitude
Y: name of column with longitude
Address: (optional) name of column with address, if included creates a column with the next address
Ward: (optional) name of column with ward number, if included creates a column with the next ward
returns: None, df is modified in place
"""
def col_for_seq(df, X, Y, Address = None, Ward = None):
    df["dwelling_num_listed"] = df.index
    df["next_dnl"] = df["dwelling_num_listed"].shift(-1)
    df["next_x"] = df[X].shift(-1)
    df["next_y"] = df[Y].shift(-1)

    if Address:
        df["next_address"] = df[Address].shift(-1)
    if Ward:
        df["next_ward"] = df[Ward].shift(-1)

    df['dist'] = df.apply(lambda row: haversine((row[X], row[Y]), (row["next_x"], row["next_y"]), unit=Unit.MILES), axis=1)
    df["num_between"] = df["next_dnl"] - df["dwelling_num_listed"] #really number between plus one


"""
Creates train and test data
data_1880: 1880 dataframe
data_1850: 1850 dataframe
cols: list of columns to include for training dataset
both: boolean, if True creates training data that combines 1880 and 1850 dataset
returns: list of train_X, train_Y, test_1880_X, test_1880_y, test_1850_X, test_1850_y
"""
def create_train_test_data(data_1880, data_1850, cols, both = True):
    data_1880 = data_1880.dropna(subset = ["house_number"])
    data_1850 = data_1850.dropna(subset = ["house_number"])
    train_1880_X, test_1880_X, train_1880_y, test_1880_y = train_test_split(data_1880.loc[:,cols], data_1880.loc[:,"house_number"], random_state = 23)

    if not both:
        return [train_1880_X, train_1880_y, test_1880_X, test_1880_y, data_1850.loc[:,cols], data_1850.loc[:,"house_number"]]
    else:
        train_1850_X, test_1850_X, train_1850_y, test_1850_y = train_test_split(data_1850.loc[:, cols],
                                                                                data_1850.loc[:, "house_number"], random_state = 23)
        return [pd.concat([train_1880_X, train_1850_X]), pd.concat([train_1880_y, train_1850_y]), test_1880_X, test_1880_y, test_1850_X, test_1850_y]

"""
Creates sequence id based on distance between consecutive dwellings
df: dataframe containing unique dwellings
d: maximum distance between dwellings, anything larger than this means a sequence break
returns list: minimum sequence length, maximum sequence length, and dataframe with sequence_id and sequence_len olumns
"""
def get_dist_seq(df, d):
    df = df.copy()
    df["sequence_id"] = np.where(df["dist"] > d, df["dist"].index, np.nan)
    df["sequence_id"].bfill(inplace = True)
    df["sequence_len"] = df.groupby("sequence_id")["num_between"].transform('sum')
    return [min(df["sequence_len"]), max(df["sequence_len"]), df]

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
                                                       ["WARD_NUM", "CENSUS_DWELLING_NUM", "sequence_id", "sequence_order", "num_between"],
                                                       ["WARD_NUM", "CENSUS_DWELLING_NUM"])

    create_street_house(dwellings_1880, "CENSUS_ADDRESS")
    create_street_house(dwellings_1850, "CD_ADDRESS")
    create_street_house(census_1880_model, "CENSUS_ADDRESS")
    create_street_house(census_1850_model, "CD_ADDRESS")

    return [dwellings_1850, dwellings_1880, census_1850_model, census_1880_model]