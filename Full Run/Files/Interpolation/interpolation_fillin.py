import pandas as pd
import numpy as np
import interpolation
# import matplotlib.pyplot as plt
# from sklearn.model_selection import train_test_split
# from sklearn.model_selection import StratifiedShuffleSplit
# from category_encoders.target_encoder import TargetEncoder
# import math
# from interpolation import dataprocessing
# from sklearn.model_selection import ShuffleSplit

"""
This file contains functions that are used to process data after the disambiguation upto
right before the model phase. See for sample use.
"""

"""
The function is to create a column that helps merging the census and the enumeration 
data.
df: df to be appended with a column
ward_col: ward column
page_col: enumeration page number column
"""
def _append_page_sequence_id(df, ward_col, page_col):
    temp = df.copy()
    temp['prev_page'] = temp.groupby(ward_col)[page_col].shift(1)
    temp['prev_ward'] = temp[ward_col].shift(1)
    temp['page_sequence_id'] = np.where((temp['prev_ward'] != temp[ward_col]) |
                                        (temp['prev_page'] > temp[page_col]), 
                                        temp[page_col].index, np.nan)
    temp['page_sequence_id'] = temp.groupby(ward_col)['page_sequence_id'].ffill()

    df_list = []
    for w in df[ward_col].unique():
        temp_df = temp.loc[temp[ward_col] == w]
        page_id = temp_df['page_sequence_id'].unique()
        page_id.sort()
        enum_list = []
        for i in range(len(page_id)):
            enum_list.append(i)
        df_list.append(pd.DataFrame({ward_col: w, 'page_sequence_id': page_id, 'new_page_sequence_id': enum_list}))
    to_new_page_id = pd.concat(df_list, axis=0)
    temp = temp.merge(to_new_page_id, on=[ward_col, 'page_sequence_id'], how='left')
    temp.drop(columns=['prev_page', 'prev_ward', 'page_sequence_id'], inplace=True)
    temp.rename(columns={'new_page_sequence_id': 'page_sequence_id'}, inplace=True)
    return temp

def merge_census_enumeration(census_df, enumeration_df, census_ward_col, census_pagenum,
                            enumeration_ward_col, enumeration_pagenum):
    
    census_2 = _append_page_sequence_id(census_df, census_ward_col, census_pagenum)
    enumerators_2 = _append_page_sequence_id(enumeration_df, enumeration_ward_col, enumeration_pagenum)
    
    ## test if keys for mergin are unique
    census_2_check = census_2[[census_ward_col, 'page_sequence_id']].groupby(census_ward_col)['page_sequence_id'].agg('nunique')
    enumerators_2_check = enumerators_2[[enumeration_ward_col, 'page_sequence_id']].groupby(enumeration_ward_col)['page_sequence_id'].agg('nunique')
    assert (census_2_check != enumerators_2_check).sum() == 0, 'Keys used for merging census and enumeration are not unique'
    
    
    ## merge census and enumeration data
    census_enumerators = census_2.merge(enumerators_2,  how = "left",
                                        left_on= [census_ward_col, census_pagenum, 'page_sequence_id'],
                                        right_on = [enumeration_ward_col, enumeration_pagenum, 'page_sequence_id'])
    census_enumerators.drop(columns=['CENSUS_PAGENNO', 'CENSUS_PAGENO_HOUSEHOLD', 'Notes'], inplace=True)

    assert census_enumerators.shape[0] == census_2.shape[0], 'Keys are not unique'
    
    return census_enumerators

"""
Return dwelling-level df with columns necessary for the fillin process
census_enum_seq: df with census and enumeration details that has been assigned sequences
ward_col: ward column
dwelling_col: dwelling id column
filled_col: name of column to be filled. The fillin info will be based on this column
"""
def get_dwelling_with_fillin_info(census_enum_seq, ward_col, dwelling_col, filled_col):
    
    census_all_dwellings = census_enum_seq.df.groupby([ward_col, dwelling_col], as_index = False).first()
    dwellings_sequence = census_all_dwellings.dropna(subset=[filled_col])

    ## 1. Get dwellings that are followed by unknown dwellings whose block num can be interpolated
    ## dwellings_sequence => known dwellings
    dwelling_sequence_sames = interpolation.same_next(dwellings_sequence, column = filled_col)
    # dwelling_sequence_sames = dwellings_sequence.groupby(ward_col).agg(interpolation.same_next, block_col)

    """
    2. Merge dwelling_sequence_sames back to all known dwelling df so that `BLOCK_NUM_next` and 
    `num_between_real` are included in df of all known dwellings.
    """
    dwellings_sequence_with_next_info = dwellings_sequence.merge(dwelling_sequence_sames[[ward_col, dwelling_col, filled_col+'_next', 'num_between_real', 'header']], on=[ward_col, dwelling_col], how='left')
    
    return census_all_dwellings, dwellings_sequence_with_next_info
