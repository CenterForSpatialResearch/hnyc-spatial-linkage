
import pandas as pd
import numpy as np
import processing as dp
import disambiguation as dl
import analysis as da
import benchmark as bm
import time

class Disambiguator:

    def __init__(self, match_df, cd_id="CD_ID", census_id="CENSUS_ID", confidence="confidence_score", census_count='census_count', lon="CD_Y", lat="CD_X", sort_var="CENSUS_ID"):
        # initialize input 
        self.input = match_df

        # initialize col names
        self.sort_var = sort_var
        self.cd_id = cd_id
        self.census_id = census_id
        self.cen_count = census_count
        self.confidence = confidence
        self.lon = lon
        self.lat = lat

        # output variables 
        self.bipartite = None
        self.results = None

    def run_disambiguation(self, cluster=True, k_between=True, cluster_kwargs = {}, path_kwargs = {}):
        start_time = time.time()
        print("Running")

        print("Creating dictionary of sub dfs (1/4)...")
        sub_groups = dp.split_dfs(self.input, self.sort_var, self.confidence)

        print("Applying algorithms iteratively (2/4)...")
        print("Number of Subgraphs: " + str(len(sub_groups)))


        # iteratively apply algorithms onto each sub df
        sub_groups = [dl.apply_algo(sub_groups, i, cluster=cluster, census_id=self.census_id, confidence=self.confidence, lat=self.lat, lon=self.lon, k_between=k_between, cluster_kwargs=cluster_kwargs, path_kwargs=path_kwargs) for i in range(0, len(sub_groups))]

        print("Cleaning output (3/4)...")
        final = pd.concat(sub_groups)

        final = final.drop_duplicates([self.cd_id, self.census_id])

        final['anchor'] = final['anchor'].fillna(0)
        final['spatial_weight'] = np.where(final['spatial_weight'].isnull(), final[self.confidence] + 1, final['spatial_weight']) # conf + 1 when weight is null - these are the rows that had did not req. spatial disambiguation, hence would def. be in shortest path

        print("Disambiguating (4/4)...")
        disambiguated = dl.get_matches(final)
        self.bipartite = disambiguated['graph']
        self.results = disambiguated['results']

        end_time = time.time()
        print("Total time:", end_time - start_time)
        print("Done! :)")
    
    def get_result(self):
        return self.results

    def get_bgraph(self):
        return self.bipartite

    def save_result(self, output_fn):
        self.results.to_csv(output_fn, index=False)

class Disambiguator1880(Disambiguator):
    def __init__(self, match_df, cd_id='CD_ID', census_id='CENSUS_ID', confidence='confidence_score', lon='LONG', lat='LAT'):
        super().__init__(match_df, cd_id=cd_id, census_id=census_id, confidence=confidence, lon=lon, lat=lat)
        self.cen_lon = None
        self.cen_lat = None
        self.cen_add = None
        self.cd_add = None

    # adding in variables needed for analysis
    def merge_census_var(self, df, merge_cen_id="CENSUS_ID"):
        try:
           self.results = self.results.merge(df, how="left", left_on=self.census_id, right_on=merge_cen_id)

        except AttributeError:
            raise Exception("Please run run_disambiguation() first")

    def merge_cd_var(self, df, merge_cd_id="CD_ID"):
        try:
            self.results = self.results.merge(df, how="left", left_on=self.cd_id, right_on=merge_cd_id)

        except AttributeError:
            raise Exception("Please run run_disambiguation() first")

    def set_var(self, var= None):
        if var is None:
            var = {'cen_lon': 'CENSUS_X', 'cen_lat': 'CENSUS_Y', 'cen_add': 'CENSUS_MATCH_ADDR', 'cd_add': 'MATCH_ADDR'}

        for key, value in var.items():
            setattr(self, key, value)

    # functions for analysis
    def get_match_rate(self):
        
        return da.get_match_rate(self.results, cd_id=self.cd_id)

    def get_addr_success(self):
        if self.cd_add is None or self.cen_add is None:
            raise Exception("Please check that cd_add and cen_add have been initialized through set_var")

        return da.get_addr_success(self.results, cd_add=self.cd_add, cen_add=self.cen_add)

    def get_dist_error(self):
        if self.cen_lon is None or self.cen_lat is None:
            raise Exception("Please check that cen_lon and cen_lat have been initialized through set_var")

        return da.get_dist_error(self.results, cen_lon=self.cen_lon, cen_lat=self.cen_lat, lon=self.lon, lat=self.lat)

    def get_under12_selections(self, age='CENSUS_AGE'):
        return da.get_under12_selections(self.results, age=age)

class Benchmark():

    def __init__(self, match, census, cd):

        #Format lat/long for census data
        self.census = self.set_census(census)
        self.cd = self.set_cd(cd)
        self.match = match
        self.benchmark = self.create_benchmark()

        #Variables to set
        self.disambiguated = None
        self.confidence = None

        #outputs
        self.benchmark_results = None
        self.confusion_matrix = None

    def set_census(self, census):
        census = census.loc[:,['CENSUS_MATCH_ADDR', 'CENSUS_Y', 'CENSUS_X']].drop_duplicates()  # select diff variables
        census.loc[census.CENSUS_Y > 1000, 'CENSUS_Y'] = 40.799935
        return census

    def set_cd(self, cd):
        return cd[['OBJECTID', 'LONG', 'LAT', "CD_BLOCK_NUM"]]

    def set_disambiguated(self, disambiguated):
        self.disambiguated = disambiguated

    def set_confidence(self, confidence):
        self.confidence = confidence
        # Need to figure out what's going on here
        self.benchmark['confidence_score'] = self.benchmark['add_match'] #+ self.benchmark[self.confidence]

    def create_benchmark(self):

        benchmark = self.match.merge(self.census, how='left', on='CENSUS_MATCH_ADDR', validate='many_to_one')

        benchmark['cd_hn'] = benchmark.apply(lambda row: bm.get_hn(row.MATCH_ADDR), axis=1)
        benchmark['cen_hn'] = benchmark.apply(lambda row: bm.get_hn(row.CENSUS_MATCH_ADDR), axis=1)
        benchmark['cd_add_cln'] = benchmark.apply(lambda row: bm.get_st(row.MATCH_ADDR), axis=1)
        benchmark['cen_add_cln'] = benchmark.apply(lambda row: bm.get_st(row.CENSUS_MATCH_ADDR), axis=1)

        benchmark['add_match'] = np.where(benchmark.cd_hn == benchmark.cen_hn, 0.1, 0) + np.where(
            benchmark.cen_add_cln == benchmark.cd_add_cln, 0.9, 0)

        benchmark.merge(self.cd, how='left', on='OBJECTID', validate='many_to_one')

        return benchmark

    def run_benchmarking(self):
        if self.confidence is None:
            raise Exception("Please set confidence first")
        if self.disambiguated is None:
            raise Exception("Please set disambiguated first")

        self.benchmark_results = da.get_dist_based_match(self.benchmark, lon = "LONG", lat = "LAT")['results']
        self.confusion_matrix = da.compare_selections(self.disambiguated, self.benchmark_results)['confusion_matrix']

    def get_benchmark(self):
        return self.benchmark

    def get_benchmark_results(self):
        if self.benchmark_results is None:
            raise Exception("Please run benchmarking first")
        return self.benchmark_results

    def get_confusion_matrix(self):
        if self.confusion_matrix is None:
            raise Exception("Please run benchmarking first")
        return self.confusion_matrix

#version for new run where cd/census information does not need to be joined in
class Benchmark_v02():

    def __init__(self, match):

        self.match = match
        self.benchmark = self.create_benchmark()

        #Variables to set
        self.disambiguated = None
        self.confidence = None

        #outputs
        self.benchmark_results = None
        self.confusion_matrix = None

    def set_disambiguated(self, disambiguated):
        self.disambiguated = disambiguated

    def set_confidence(self, confidence):
        self.confidence = confidence
        # Need to figure out what's going on here
        self.benchmark['confidence_score'] = self.benchmark['add_match'] #+ self.benchmark[self.confidence]

    def create_benchmark(self):
        benchmark = self.match.copy()

        #change to use actual street/house numbers once they are sorted out for the census data
        benchmark['cd_hn'] = benchmark.apply(lambda row: bm.get_hn(row.CD_H_ADDRESS), axis=1)
        benchmark['cen_hn'] = benchmark.apply(lambda row: bm.get_hn(row.CENSUS_MATCH_ADDR), axis=1)
        benchmark['cd_add_cln'] = benchmark.apply(lambda row: bm.get_st(row.CD_H_ADDRESS), axis=1)
        benchmark['cen_add_cln'] = benchmark.apply(lambda row: bm.get_st(row.CENSUS_MATCH_ADDR), axis=1)

        benchmark['add_match'] = np.where(benchmark.cd_hn == benchmark.cen_hn, 0.1, 0) + np.where(
            benchmark.cen_add_cln == benchmark.cd_add_cln, 0.9, 0)

        return benchmark

    def run_benchmarking(self):
        if self.confidence is None:
            raise Exception("Please set confidence first")
        if self.disambiguated is None:
            raise Exception("Please set disambiguated first")

        self.benchmark_results = da.get_dist_based_match(self.benchmark, confidence = self.confidence)['results']
        self.confusion_matrix = da.compare_selections(self.disambiguated, self.benchmark_results)['confusion_matrix']

    def get_benchmark(self):
        return self.benchmark

    def get_benchmark_results(self):
        if self.benchmark_results is None:
            raise Exception("Please run benchmarking first")
        return self.benchmark_results

    def get_confusion_matrix(self):
        if self.confusion_matrix is None:
            raise Exception("Please run benchmarking first")
        return self.confusion_matrix