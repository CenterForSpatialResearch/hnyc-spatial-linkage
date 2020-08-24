import pandas as pd
import interpolation.interpolation as interpolation
import interpolation.sequences as sequences

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
Takes whole data set and dwellings level data set and joins them
all: dataset with all entries
cols: list of column names to include from dwellings level dataset
joins: list of columns to perform the join on
return: dataframe of all census with dwellings info
"""
def dwellings_to_all(all, dwellings, cols, joins):
    df = all.merge(dwellings.loc[:,cols], how = "left", on = joins, validate = 'many_to_one')
    return df

"""
Purpose: Combine dwellings with addresses, with added sequences and all dwellings
all_dwellings: dataframe with all dwellings
known_dwellings: dataframe with known_dwellings
returns: dataframe with all dwellings, with sequence ids, order_enum, order
"""
def all_dwellings_sequenced(all_dwellings, known_dwellings, ward_col = "CENSUS_WARD_NUM", dwelling_col = "CENSUS_DWELLING_NUM"):
    known_dwellings["Known"] = 1
    prediction_data = dwellings_to_all(all_dwellings, known_dwellings, list(
        set(list(known_dwellings.columns)).difference(list(all_dwellings.columns))) + [ward_col,
                                                                                       dwelling_col],
                                       [ward_col, dwelling_col])

    prediction_data["sequence_id"] = prediction_data["sequence_id"].ffill()
    prediction_data["sequence_order_enum"] = prediction_data["sequence_order_enum"].ffill()
    prediction_data = prediction_data.groupby("sequence_id").apply(sequences.sequence_order)
    return prediction_data









