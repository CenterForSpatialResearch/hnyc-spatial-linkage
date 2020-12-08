import pandas as pd
import numpy as np
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
def dwellings_to_all(all_df, dwellings, cols, joins):
    df = all_df.merge(dwellings.loc[:,cols], how = "left", on = joins, validate = 'many_to_one')
    return df


"""
Check if df starts and ends with the same value of check_column. If so, interpolate
all rows in between. Otherwise, return the input.
df: df of dwellings whose first and last rows are known dwellings. Must contain only 1
      `consecutive_dwelling_id`
check_column: column to check for the start and end values and to fill in. This is intended
    to be sequence column.
"""
def fill_in(df, fill_column, check_column):
    fill = True
    if not isinstance(check_column, list):
        raise TypeError("name 'check_column' must be list.")
    for col in check_column:
        if df[col].values[0] != df[col].values[-1]:
            fill = False
            break
    if fill:
        df[fill_column] = df[fill_column].ffill()
    return df


"""
Purpose: Combine dwellings with addresses, with added sequences and all dwellings
all_dwellings: dataframe with all dwellings
known_dwellings: dataframe with known_dwellings
fill_column: columns to be fill in. Usually, sequence columns
check_column: columns to check for fill in. Usually, the same as fill_column
returns: dataframe with all dwellings, with sequence ids, order_enum, order
"""
def all_dwellings_sequenced(all_dwellings, known_dwellings, block_col, fill_column, check_column,
                            ward_col = "CENSUS_WARD_NUM", 
                            dwelling_col = "CENSUS_DWELLING_NUM",
                            dwelling_max=None):
#     known_dwellings["Known"] = 1
    ## join all dwellings and known dwellings together
    prediction_data = dwellings_to_all(all_dwellings, known_dwellings, list(
        set(list(known_dwellings.columns)).difference(list(all_dwellings.columns))) + [ward_col,
                                                                                       dwelling_col],
                                       [ward_col, dwelling_col])

    ## fill in wherever makes sense
    #### 1. get rows of unknown dwellings that are in between the same block_col value
    cons_dwellings = interpolation.get_consecutive_dwellings(prediction_data, column = fill_column) 
    
    #### 2. if rows are also in between the same sequence, fill in the sequence
    ## dwelling_max is set, only interpolate those that fulfill.
    if dwelling_max is not None:
        cons_dwellings = interpolation.limit_dwellings_between(cons_dwellings, dwelling_max)
    interpolated_unknown = cons_dwellings.groupby('consecutive_dwelling_id').apply(fill_in, fill_column, check_column)
    interpolated_unknown.drop(columns='consecutive_dwelling_id', inplace=True)
    
    interpolated_unknown.drop_duplicates(inplace=True) ## dwellings that start and end will be removed
    
#     prediction_data["sequence_order_enum"] = prediction_data["sequence_order_enum"].ffill()  <<< should be put in fill_in()
#     prediction_data = prediction_data.groupby("sequence_id").apply(sequences.sequence_order) <<< do not know what this is 11.24
    
    ## Merge back to all dwelling data
    prediction_data = dwellings_to_all(prediction_data, interpolated_unknown, list(
        set(list(interpolated_unknown.columns)).difference(list(prediction_data.columns))) + [ward_col,
                                                                                       dwelling_col, fill_column],
                                       [ward_col, dwelling_col])
    prediction_data[fill_column] = np.where(prediction_data[fill_column+'_x'].isnull(), 
                                            prediction_data[fill_column+'_y'], 
                                            prediction_data[fill_column+'_x'])
    ##must bring back sequence_order_enum as well ^^^
    
    ## Drop helper columns
    prediction_data.drop(columns=[fill_column+'_x', fill_column+'_y'], inplace=True)
    

    return prediction_data

"""
Purpose: generate dwelling id that's unique for every dwelling
df: dataframe with census data
dwelling_col: column with dwelling number information from census
"""

def create_unique_dwelling(df, dwelling_col = "CENSUS_DWELLING_NUM"):
    dwelling = df[dwelling_col].iloc[0]
    dwelling_num = 1
    dwelling_id = []
    for row in df.itertuples():
        row_dwelling = getattr(row, dwelling_col)
        if row_dwelling == dwelling:
            dwelling_id.append(dwelling_num)
        if row_dwelling != dwelling:
            dwelling_num += 1
            dwelling_id.append(dwelling_num)
            dwelling = row_dwelling

    df["dwelling_id"] = dwelling_id
    return df








