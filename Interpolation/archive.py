import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from interpolation import sequences, dataprocessing

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

    sequences.col_for_seq(dwellings_1850, "CD_X", "CD_Y")
    sequences.col_for_seq(dwellings_1880, "POINT_X", "POINT_Y")

    dwellings_1850 = sequences.get_dist_seq(dwellings_1850, 0.15)[2]
    dwellings_1880 = sequences.get_dist_seq(dwellings_1880, 0.15)[2]

    dwellings_1850 = dwellings_1850.groupby("sequence_id").apply(sequences.sequence_order)
    dwellings_1880 = dwellings_1880.groupby("sequence_id").apply(sequences.sequence_order)

    #Not super sure what's happening here, come back and check this
    dwellings_1850 = dwellings_1850.groupby(["WARD_NUM", "CENSUS_DWELLING_NUM"], as_index=False).first()
    dwellings_1880 = dwellings_1880.drop_duplicates(subset=["CENSUS_ADDRESS"]).reset_index(drop=True).copy()

    census_1880_model = dataprocessing.dwellings_to_all(census_1880, dwellings_1880,
                                                       ["CENSUS_MATCH_ADDR", "sequence_id", "sequence_order", "num_between"], ["CENSUS_MATCH_ADDR"])
    census_1850_model = dataprocessing.dwellings_to_all(census_1850, dwellings_1850,
                                                       ["WARD_NUM", "CENSUS_DWELLING_NUM", "sequence_id", "sequence_order", "num_between", "sequence_order_enum"],
                                                       ["WARD_NUM", "CENSUS_DWELLING_NUM"])

    dataprocessing.create_street_house(dwellings_1880, "CENSUS_ADDRESS")
    dataprocessing.create_street_house(dwellings_1850, "CD_ADDRESS")
    dataprocessing.create_street_house(census_1880_model, "CENSUS_ADDRESS")
    dataprocessing.create_street_house(census_1850_model, "CD_ADDRESS")

    return [dwellings_1850, dwellings_1880, census_1850_model, census_1880_model]