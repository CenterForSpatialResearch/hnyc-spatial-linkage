from haversine import haversine, Unit
import numpy as np

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
This takes a dataframe of a single sequence and creates a sequence_order column, designed to be used with groupby("sequence_id").apply()
df: dataframe with a single sequence, including "num_between" column
returns: dataframe with sequence_order column
"""
def sequence_order(df):
    df["sequence_order"] = df["num_between"].cumsum()
    return df

"""
Creates sequence id based on distance between consecutive dwellings
df: dataframe containing unique dwellings
d: maximum distance between dwellings, anything larger than this means a sequence break
returns: dataframe
"""
def get_dist_seq(df, d):
    print('d: ', d)
    df = df.copy()
    df["sequence_id"] = np.where(df["dist"] > d, df["dist"].index, np.nan)
    df["sequence_id"].iloc[-1] = df.tail(1).index[0]
    df["sequence_id"].bfill(inplace = True)
        
#     #deal with last dwelling
#     try:
#         if df["dist"].iloc[-2] > d:
#             df["sequence_id"].iloc[-1] = df.tail(1).index[0] ## the last dwelling starts a new sequence
#         else:
#             df["sequence_id"].ffill(inplace = True)
#     except:
#         df["sequence_id"] = df.iloc[0].index[0]

#     #handle if every distance in ward is less that d
#     if df["sequence_id"].isnull().all():
#         df["sequence_id"] = df.tail(1).index[0]

    df["sequence_len"] = df.groupby("sequence_id")["num_between"].transform('sum')
    sequence_map = {ids:num for ids, num in zip(df["sequence_id"].unique(), range(len(df["sequence_id"].unique())))}
    df["sequence_order_enum"] = df.apply(lambda row: sequence_map[row["sequence_id"]], axis = 1)
    return df

# def get_dist_seq(df, d):
#     print('d: ', d)
#     df = df.copy()
#     df["sequence_id"] = np.where(df["dist"] > d, df["dist"].index, np.nan)
#     df["sequence_id"].bfill(inplace = True)

#     #deal with last dwelling
#     try:
#         if df["dist"].iloc[-2] > d:
#             df["sequence_id"].iloc[-1] = df.tail(1).index[0]
#         else:
#             df["sequence_id"].ffill(inplace = True)
#     except:
#         df["sequence_id"] = df.iloc[0].index[0]

#     #handle if every distance in ward is less that d
#     if df["sequence_id"].isnull().all():
#         df["sequence_id"] = df.tail(1).index[0]

#     df["sequence_len"] = df.groupby("sequence_id")["num_between"].transform('sum')
#     sequence_map = {ids:num for ids, num in zip(df["sequence_id"].unique(), range(len(df["sequence_id"].unique())))}
#     df["sequence_order_enum"] = df.apply(lambda row: sequence_map[row["sequence_id"]], axis = 1)
#     return df

"""
Purpose: Tunes selection of maximum distance between known consecutive sequences, but minimizing the difference in sequence
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
Purpose: Create fixed length (number of dwellings) sequence
df: dataframe with unique dwellings (all dwellings in dataset
n: number of dwellings in a sequence
"""
def fixed_len_seq(df, n = 40):
    val = 0
    count = 0
    seq = []
    for i in range(len(df)):
        if count < n:
            count += 1
            seq.append(val)
        else:
            count = 0
            val += 1
            seq.append(val)

    df["fixed_seq"] = seq
    return df

"""
Purpose: Get CENSUS_DWELLING_NUM based sequences
returns: dataframe with dwelling_seq_id column
"""
def get_dwelling_seq(df, dwelling_col_num = "CENSUS_DWELLING_NUM"):
    df["dwelling_seq_id"] = np.where(df[dwelling_col_num] == 1, df[dwelling_col_num].index, np.nan)
    df["dwelling_seq_id"].ffill(inplace= True)
    return df





