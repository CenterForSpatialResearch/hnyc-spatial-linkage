import pandas as pd
import numpy as np
from collections import defaultdict

"""
Purpose: Add in diambiguation information to census data
disamb: Disambiguation dataframe
census: Census dataframe
disamb_columns: columns to add from disambiguation to census data
disamb_id: unique id in disambiguaiton data set to join on
census_id: unique id in census dataset to join on
"""
def add_disamb_census(disamb, census, disamb_columns = None, disamb_id = "CENSUS_ID", census_id = "CENSUS_IPUMS_UID"):
    if disamb_columns is None:
        disamb_columns = ["CENSUS_ID", "CD_ADDRESS", "spatial_weight", "CD_X", "CD_Y", "BLOCK_NUM"]

    #Get selected matches and relevent columns from the disambiguation data
    disamb_selected = disamb[disamb["selected"] == 1].copy()
    disamb_selected = disamb_selected.loc[:, disamb_columns].copy()

    if disamb_id == "CENSUS_ID":
        disamb_selected.loc[:, "CENSUS_ID"] = disamb_selected["CENSUS_ID"].apply(lambda x: x.strip("CENSUS_"))

    return census.merge(disamb_selected, how="left", left_on=census_id, right_on=disamb_id)

"""
Purpose: Get percentage of nonnull values in specific column of dataframe
df: dataframe
name: column to get rate of
total: total number of values, if none, the total is considered to be the length of the dataframe
"""
def get_match_rate(df, name = "CD_ADDRESS", total = None):
    if total is None:
        total = len(df)
    return (df[name].count()/total)


"""
Purpose: Display match rates at the ward level
df: dataframe
ward_col: name of ward column
name: column to get the rate of 
total: total number of values, if none the total is considered to be the length of the dataframe
"""
def get_ward_match(df, ward_col = "CENSUS_WARD_NUM", name = "CD_ADDRESS", total = None):
    for ward, df_ward in df.groupby(ward_col):
        print("Ward ",str(ward),": ", round(get_match_rate(df_ward, name = name, total = total), 5))

"""
Purpose: Getting counts for disambiguation dwelling conflicts
df: df with records from a single ward
ward: ward number column
dwelling: dwelling number column
address: address from linkage column
"""
def get_counts(df, ward_col = "CENSUS_WARD_NUM", dwelling_col = "CENSUS_DWELLING_NUM", address = "CD_ADDRESS"):

    #save dwellings
    no_add = defaultdict(int)
    one_add = defaultdict(int)
    more_add = defaultdict(int)

    #save count of total records
    no_add_counts = 0
    one_add_counts = 0
    more_add_counts = 0

    for index, x in df.groupby([ward_col, dwelling_col]):
        c = x[address].nunique()
        if c == 0:
            no_add[index[0]] += 1
            no_add_counts += len(x)
        elif c == 1:
            one_add[index[0]] += 1
            one_add_counts += len(x)
        elif c > 1:
            more_add[index[0]] += 1
            more_add_counts += len(x)

    return {"no_address":(no_add, no_add_counts), "single_address":(one_add, one_add_counts), "multiple_addresses":(more_add, more_add_counts)}

"""
Purpose: Generate census data with conflicts resolved by max spatial weight sum, designed for use with groupby.apply
x: dataframe with single dwelling
address: address column
block_num: block number column
lat: latitude column
long: longitude column
"""

def dwelling_weight_fill(x, address = "CD_ADDRESS", block_col = "BLOCK_NUM", lat = "CD_X", long = "CD_Y"):
    if x[address].count() > 0:
        x["spatial_weight_sum"] = x.groupby([address])['spatial_weight'].transform('sum')
        x.reset_index(drop=True, inplace=True)
        index = x["spatial_weight_sum"].idxmax()

        x[address] = x.iloc[index].loc[address]
        x[block_col] = x.iloc[index].loc[block_col]
        x[lat] = x.iloc[index].loc[lat]
        x[long] = x.iloc[index].loc[long]

    return x

