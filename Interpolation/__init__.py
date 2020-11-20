import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import interpolation.interpolation as interpolation
from copy import deepcopy
import interpolation.sequences as sequences
import interpolation.dataprocessing as dataprocessing
from functools import reduce

from kmodes.kmodes import KModes
from sklearn.compose import ColumnTransformer

#Store and modify census data post disambiguation and dwelling fillin/conflict resolution
class CensusData:

    def __init__(self, data, ward_col="CENSUS_WARD_NUM", dwelling_col="dwelling_id", dwelling_col_num  = "CENSUS_DWELLING_NUM", block_col = "CD_BLOCK_NUM", x_col = "CD_X", y_col = "CD_Y", pagenum = "CENSUS_PAGENUM"):

        #hold census data post disambiguation and dwelling fill in/conflict resolution
        self.data = data

        # initialize col names
        self.ward_col = ward_col
        self.dwelling_col = dwelling_col
        self.block_col = block_col
        self.dwelling_col_num = dwelling_col_num
        self.x_col = x_col
        self.y_col = y_col
        self.pagenum = pagenum

        #holds data processesed as desired
        self.df = None

    def get_dwellings(self):
        return self.data.groupby([self.ward_col, self.dwelling_col], as_index = False).first()

    """
    Purpose: Add distance based sequences to dwelling data
    d: maximum distance between dwellings within a sequence
    returns: sequences added to dwelling data
    """
    def get_dwellings_dist_seq(self, d):
        print('d: ', d)
        dwellings = self.get_dwellings()
        dwellings.dropna(subset = [self.block_col], inplace = True)
        dwellings_cols = dwellings.groupby(self.ward_col, as_index=False).apply(
            lambda x: sequences.col_for_seq(x, X=self.x_col, Y=self.y_col))
        dwellings_cols = dwellings_cols.groupby(self.ward_col, as_index=False).apply(
            lambda x: sequences.get_dist_seq(x, d))          
            
        return dwellings_cols.loc[:, [self.ward_col, self.dwelling_col, "sequence_id", "num_between",
                                              "sequence_order_enum", "dist", "sequence_len"]].copy()
    
    def get_dwellings_dist_seq_after(self, d):
        print('d: ', d)
        dwellings = self.get_dwellings()
        dwellings.dropna(subset = [self.block_col], inplace = True)
        dwellings_cols = dwellings.groupby(self.ward_col, as_index=False).apply(
            lambda x: sequences.col_for_seq(x, X=self.x_col, Y=self.y_col))
        dwellings_cols = dwellings_cols.groupby(self.ward_col, as_index=False).apply(
            lambda x: sequences.get_dist_seq_after(x, d))          
            
        return dwellings_cols.loc[:, [self.ward_col, self.dwelling_col, "sequence_id", "num_between",
                                              "sequence_order_enum", "dist", "sequence_len"]].copy()

#     """
#     fill sequences of unknown records that are in between 2 known records of the same sequence
#     """
#     def fill_within(self, df, dwelling_col, seq_col):
#         min_dwelling_id = min(df[dwelling_col])
#         max_dwelling_id = max(df[dwelling_col])
#         dwelling_id_df = pd.DataFrame({dwelling_col: range(min_dwelling_id, max_dwelling_id+1)})

#         dwellings_cols = df.merge(dwelling_id_df, on=dwelling_col, how='right')
#         dwellings_cols.sort_values(dwelling_col, inplace=True)
#         dwellings_cols.reset_index(inplace=True)
#         dwellings_cols['ffill'] = dwellings_cols[seq_col].ffill()
#         dwellings_cols['bfill'] = dwellings_cols[seq_col].bfill()
#         dwellings_cols[seq_col] = np.where(dwellings_cols['ffill'] == dwellings_cols['bfill'], dwellings_cols['bfill'], dwellings_cols[seq_col])
#         dwellings_cols.drop(columns=['ffill', 'bfill'], inplace=True)
#         return dwellings_cols
        
    """
    Purpose: Create dataframe of all census records with sequences added in
    d: Maximum distance between dwellings within the same sequence
    tuned: Dataframe of dwelling with sequences -- allows user to feed in dataframe generated during max distance tuning
     if that's done first. If None (default) generates dataframe of dwellings with sequences with provided max distance
    """
    def census_records_with_seq(self, d = 0.1, tuned = None):
        if tuned:
            dwellings_sequenced = tuned
        else:
            dwellings_sequenced = self.get_dwellings_dist_seq(d)

        census_1850_model = dataprocessing.dwellings_to_all(self.data, dwellings_sequenced,
                                             [self.ward_col, self.dwelling_col, "sequence_id", "num_between",
                                              "sequence_order_enum"],
                                             [self.ward_col, self.dwelling_col])
        census_1850_model.dropna(inplace=True, subset=["sequence_id"])
        self.df = census_1850_model

    '''
    Purpose: Get sequences with n dwellings each (the last one will have remaining dwellings)
    n: number of dwellings in sequence
    returns: dwellings with ward column, dwelling column, and fixed_seq column (individual dwellings only)
    '''
    def get_dwellings_fixed_seq(self, n = 40):
        dwellings = self.get_dwellings()
        dwellings_fixed = dwellings.groupby(self.ward_col, as_index = False).apply(lambda x: sequences.fixed_len_seq(x, n))
        return dwellings_fixed.loc[:, [self.ward_col, self.dwelling_col, "fixed_seq"]]

    """
    Purpose: Get sequences based on CENSUS_DWELLING_NUM
    returns: dataframe with ward column, dwelling column, and dwelling_seq _id (individual dwellings only) 
    """
    def get_dwellings_dwellings_seq(self):
        dwellings = self.get_dwellings()
        dwellings_dwellings_seq = dwellings.groupby(self.ward_col, as_index = False).apply(lambda x: sequences.get_dwelling_seq(x, self.dwelling_col_num))
        return dwellings_dwellings_seq.loc[:, [self.ward_col, self.dwelling_col, "dwelling_seq_id"]]

    #new forms of sequence creation can be applied here
    """
    Purpose: apply specified sequencing
    d: maximum distance, for distance, enumerator_dist sequences
    n: number of dwellings, for fixed sequences
    distance: distance based sequences, divided based on max consecutive distance
    fixed: sequences with n dwellings
    dwelling: sequences based on CENSUS_DWELLING_NUM column from data
    enumerator: sequences based on dwellings visited by a given enumerator in a single day
    tuned: distance sequences were tuned
    enumerator_dist: distance based sequences, built within enumerator sequences (rather than simply within wards)
    sets self.df to a dataframe with specified sequences and all census records
    """
    def apply_sequencing(self, after, d = 0.1, n = 40, distance = False, fixed = False, dwelling = False, enumerator = False,tuned = False, enumerator_dist = False):
        print('d: ', d)
        sequences_dfs = []
        if distance:
            if after:
                dwellings_dist = tuned if tuned else self.get_dwellings_dist_seq_after(d)    
            else:
                dwellings_dist = tuned if tuned else self.get_dwellings_dist_seq(d)            
            sequences_dfs.append(dwellings_dist)

        if fixed:
            sequences_dfs.append(self.get_dwellings_fixed_seq(n))

        if dwelling:
            sequences_dfs.append(self.get_dwellings_dwellings_seq())

        if enumerator:
            sequences_dfs.append(self.get_enum_seq())

        if enumerator_dist:
            dwellings_dist = self.get_enum_seq(all = True)
            dwellings_dist.dropna(subset=[self.block_col], inplace=True)
            dwellings_dist = dwellings_dist.groupby("enum_seq", as_index=False).apply(
                lambda x: sequences.col_for_seq(x, X=self.x_col, Y=self.y_col))
            if after:
                dwellings_dist = dwellings_dist.groupby("enum_seq", as_index=False).apply(
                    lambda x: sequences.get_dist_seq_after(x, d))
            else:
                dwellings_dist = dwellings_dist.groupby("enum_seq", as_index=False).apply(
                lambda x: sequences.get_dist_seq(x, d))

            dwellings_dist.rename(columns = {"sequence_id":"enum_dist_id", "sequence_order_enum":"enum_dist_order", "dist":"enum_dist", "sequence_len":"enum_sequence_len"}, inplace = True)
            sequences_dfs.append(dwellings_dist.loc[:, [self.ward_col, self.dwelling_col, "enum_dist_id", "enum_dist_order", "enum_dist", "enum_sequence_len"]].copy())

        self.df = reduce(lambda x, y: pd.merge(x, y, how = "left", on = [self.ward_col, self.dwelling_col]), sequences_dfs, self.data)

        ## fill within for distance sequence
#         if distance:
#             self.df = self.df.groupby(self.ward_col, as_index=False).apply(lambda x: self.fill_within(x, self.dwelling_col, 'sequence_id'))
        
        if enumerator_dist:
#             self.df = self.df.groupby(self.ward_col, as_index=False).apply(lambda x: self.fill_within(x, self.dwelling_col, 'enum_dist_id'))
            self.df = self.df.dropna(subset = [self.pagenum])
        
    """
    Purpose: Get sequences of dwellings visited by given enumerator in a single day
    enum_num: column name of census enumerator label
    date: column name of date
    all: whether to include all census records or not
    """
    def get_enum_seq(self, enum_num = "CENSUS_ENUMERATOR_NUM", date = "CENSUS_ENUMERATOR_DATE", all = False):
        dwellings = self.get_dwellings()
        with_labels = []
        enum_label = 1
        for id, df_org in dwellings.groupby([enum_num, date]):
            df = df_org.copy()
            df["enum_seq"] = enum_label
            enum_label += 1
            with_labels.append(df)
        final_dwells = pd.concat(with_labels)
        if all:
            return final_dwells

        return final_dwells.loc[:,[self.ward_col, self.dwelling_col, "enum_seq"]]

    """

    """
    def census_dwelling_records_between(self, d = 0.1, column = None, tuned = None):
        if column is None:
            column = self.block_col
        if tuned:
            dwellings_sequenced = tuned
        else:
            dwellings_sequenced = self.get_dwellings_dist_seq(d)

        dwelling_sequence_sames = interpolation.same_next(dwellings_sequenced, column=column)
        all_dwellings = dataprocessing.all_dwellings_sequenced(self.data, dwelling_sequence_sames,
                                                              ward_col=self.ward_col, dwelling_col=self.dwelling_col)
        self.df = interpolation.get_consecutive_dwellings(all_dwellings, column=column)

    def no_seq(self):
        self.df = self.data

    """
    Purpose: group records based on their similarity in certain columns
    sim_columns: List of column names that will be used to measure the similarity
    
    The current self.df must contain only 1 ward
    """
    def apply_similarity(self, sim_columns, k=20):
        
        dwellings = self.df.groupby([self.ward_col, self.dwelling_col], as_index = False).first()#.copy()
        similarity_df = dwellings[sim_columns+[self.dwelling_col]].copy()
        similarity_df.fillna(value=-1, inplace=True)
        ## take only columns to be clustered
        ## unknown values are treated as a new category
#         similarity_df = self.df[sim_columns].copy()
#         similarity_df.fillna(value=-1, inplace=True)
        
        ## process data fro Kmodes. Convert all columns into string
        for c in sim_columns:
            similarity_df[c] = similarity_df[c].astype('str')
            
        kmodes_model = KModes(n_clusters=k, init = "Cao", n_init = 1, verbose=1)
        kmodes_pred = kmodes_model.fit_predict(similarity_df[sim_columns], 
                                                   categorical=[similarity_df.columns.get_loc(c) for c in sim_columns])
        
        similarity_df['similarity_label'] = kmodes_pred
        similarity_df.drop(columns=sim_columns, inplace=True)
        self.df = self.df.merge(similarity_df, how = "left", on = self.dwelling_col)

#Base class for interpolation, not meant to be instantiated
class Interpolator:

    def __init__(self, census_data, ward, model, feature_names, *args):

        # initialize input
        self.ward = ward #ward within which the interpolator is predicting
        self.model = model #model for prediction
        self.feature_names = feature_names #feature names used for prediction
        self.y = None  # column name of value to predict
        self.df = census_data.df[census_data.df[census_data.ward_col] == ward].copy() #get data within ward as a dataframe

        # initialize col names
        self.ward_col = census_data.ward_col
        self.dwelling_col = census_data.dwelling_col
        self.block_col = census_data.block_col
        self.x_col = census_data.x_col
        self.y_col = census_data.y_col

        # results
        self.train_score = None
        self.test_score = None

    """
    Purpose: Get a train test data, with dwellings only present in one or the other
    Stratified: If true stratify sample, if false, don't
    """
    def stratified_train_test(self, stratified = True, k=1):
        return interpolation.stratified_train_test(self.df, self.y, self.dwelling_col, stratified,k=k)

    """
    Purpose: Target encode train, test data
    Train: train data
    Test: test data
    """
    def target_encoder(self, train, test):
        return interpolation.target_encoder(train, test, self.feature_names,self.y)

    """
    Purpose: Train model, and save train and test scores
    train: train data
    test: test data
    train_y: train data to predict, if None, assumes that this is in the test datasets
    test_y: test data to predict, if None assumeds that this is in the test dataset
    """
    def train_test_model(self, train, test, train_y = None, test_y = None):

        if train_y is None:
            tr = train#.loc[:,self.feature_names + ['block_num']] ## block_num for later ref. Actually dropped when .fit()
            tr_y = train[self.y]
            te = test#.loc[:, self.feature_names + ['block_num']]
            te_y = test[self.y]
            self.model.fit(tr, tr_y)
            self.train_score = self.model.score(tr, tr_y)
            self.test_score = self.model.score(te, te_y)

        else:
            tr = train#.loc[:, self.feature_names+ ['block_num']]
            te = train#.loc[:, self.feature_names+ ['block_num']]
            self.model.fit(tr, train_y)
            self.train_score = self.model.score(tr, train_y)
            self.test_score = self.model.score(te, test_y)
            
    def cross_validate_model(self, k=10, stratified=True):
        
        train_list, test_list = self.stratified_train_test(k=k, stratified=stratified)
        self.train_score = []
        self.test_score = []
        for i in range(k):
            tr = train_list[i]#.loc[:,self.feature_names+ ['block_num']]
            tr_y = train_list[i][self.y]
            te = test_list[i]#.loc[:, self.feature_names+ ['block_num']]
            te_y = test_list[i][self.y]
#             print(tr.shape, tr_y.shape, te.shape, te_y.shape)
#             print(self.model)
            self.model.fit(tr, tr_y)
            self.train_score.append(self.model.score(tr, tr_y))
            self.test_score.append(self.model.score(te, te_y))
#         self.temp_train_list = train_list ## Check for info leak in cross validation
#         self.temp_test_list = test_list ## Check for info leak in cross validation
        self.last_te = te
        self.last_te_y = te_y

    """
    Purpose: Use model for predicting values after training
    data: data to predict on, assumed to have columns in feature_names attribute
    returns: predictions
    """
    def predict(self, data):
        return self.model.predict(data.loc[:,self.feature_names])

    """
    Purpose: reset model, primarily for tuning/development, can be done directly, but use this to add awareness to
    modifications
    model: model to give the function 
    """
    def set_model(self, model):
        self.model = model

    """
    Purpose: reset feature names, primarily for development. Used for awareness to modification. 
    *Note, if using pipeline with column specific preprocessing, this will cause a failure. Must do preprocessing 
    outside of the model, and set model as a model without pipeline, or change pipeline preprocessing columns appropriately
    and set model with new pipeline
    features: list of features to train on
    """
    def set_features(self, features):
        self.feature_names = features

#Predict block number
class BlockInterpolator(Interpolator):
    def __init__(self, census_data, ward, model, feature_names):
        super().__init__(census_data, ward, model, feature_names)
        self.y = self.block_col #object designed to predict block number
        self.df.dropna(subset=[self.y], inplace=True)

#Predict centroids
class CentroidInterpolator(Interpolator):

    def __init__(self, census_data, ward, model, feature_names, clustering_algo, block_centroids):
        super().__init__(census_data, ward, model, feature_names)
        self.y = "cluster" #predicts clustering label generated by clustering algorhtm
        self.df.dropna(subset=[self.block_col], inplace=True)

        #clustering variables
        self.clustering_algo = clustering_algo #clustering algorithm
        self.block_centroids = block_centroids #nested dictionary {ward:{block:centroids,...}...}

        #Variables to set after clustering
        self.block_cluster_map = None #saves dictionary of {block:cluster} generated through clustering algorithm
        self.clusters = None #saves list of clusters in order of block centroids

    """
    Purpose: Clustering block centroids
    algo_fit: If true don't fit the clustering algorithm, just predict
    """
    def apply_clustering(self,algo_fit=False):

        to_cluster = np.array(list(self.block_centroids[self.ward].values()))

        if algo_fit:
            self.clusters = self.clustering_algo.predict(to_cluster)
        else:
            self.clusters = self.clustering_algo.fit_predict(to_cluster)

        self.block_cluster_map = {block: clust for block, clust in zip(self.block_centroids[self.ward].keys(), self.clusters)}
        self.df["cluster"] = self.df.apply(lambda row: self.block_cluster_map[row[self.block_col]], axis=1)

    """
    Purpose: General tuning of clustering algorithm
    algo: Clustering algorithm to use
    clusteringvis: If true visualize clusters
    model_score: If true train model
    stratified: If true stratify sample when creating training/test sets, irrelevant if model_scores is False
    *Note: assumes model is a pipeline that includes preprocessing
    """
    def clustering_algo_tuning(self, algo, clustervis = True, model_scores = True, stratified = True):
        self.set_clustering_algo(algo)
        self.apply_clustering()
        if clustervis:
            self.clustervis()
        if model_scores:
            train, test = self.stratified_train_test(stratified = stratified)
            self.train_test_model(train, test)


    """
    Purpose: Visualize clustering, points represent census data
    kmeans: If true assumes clustering algorithm is KMeans and plots cluster centroids
    title: Title of visualization, if None uses default
    """
    def clustervis(self, kmeans=False, title=None):
        if self.clusters is None:
            raise AttributeError("Please run apply clustering first")

        #colors = [color for color, i in zip(mcolors.CSS4_COLORS.values(), range(len(np.unique(self.clusters)))) if i < len(np.unique(self.clusters))]
        colors = [plt.rcParams['axes.prop_cycle'].by_key()['color'][x] for x in range(len(np.unique(self.clusters)))]
        fig, ax = plt.subplots(1, 1, figsize=(8, 8))

        for cluster, color in zip(np.unique(self.clusters), colors):
            # get cluster data
            X_subset = self.df[self.df["cluster"] == cluster]
            centroids_subset = np.array(list(self.block_centroids[self.ward].values()))[self.clusters == cluster]

            # graph info
            ax.scatter(x=X_subset.loc[:, self.x_col], y=X_subset.loc[:, self.y_col], s=5, alpha=0.8, label=str(cluster),
                       color=color)
            ax.scatter(centroids_subset[:, 0], centroids_subset[:, 1], marker="*", s=80, color=color)
            if title is not None:
                ax.set_title(title)
            else:
                ax.set_title("Clustered block centroids, Ward {}, 1850".format(self.ward))
        if kmeans:
            ax.scatter(x=self.clustering_algo.cluster_centers_[:, 0], y=self.clustering_algo.cluster_centers_[:, 1], marker="x", c="k", s=50,
                       label="cluster centroids")
        ax.legend(bbox_to_anchor = (1.04, 1), loc = "upper left")
        plt.show()

    """
    Purpose: Handle instability in kmeans results by saving the best model and best score after 100 runs
    n: number of clusters
    *Note assumes model is a pipeline that includes preprocessing
    * 10/31: change to CV when evaluate best k
    """
    def kmeans_best(self, n):

        self.set_clustering_algo(KMeans(n))

        score = 0
        best_clusterer = None
        for i in range(50):

            self.apply_clustering()
            self.cross_validate_model()
            
            
#             train, test = self.stratified_train_test()
#             self.train_test_model(train, test)

            if np.mean(self.test_score) > score:
                score = np.mean(self.test_score) 
                best_clusterer = deepcopy(self.clustering_algo)

            if (i+1) % 10 == 0:
                print("n is {} and it's the {}th iteration".format(n, i+1))

        return score, best_clusterer

    """
    Purpose: reset clustering algorithm
    clustering_algo: clustering algorithm to set data
    """
    def set_clustering_algo(self, clustering_algo):
        self.clustering_algo = clustering_algo


