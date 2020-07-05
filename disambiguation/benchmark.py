import pandas as pd
import re
import numpy as np
import disambiguation.analysis as da

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
        census = census.loc[:,['CENSUS_MATCH_ADDR', 'CENSUS_X', 'CENSUS_Y']].drop_duplicates()  # select diff variables
        census.loc[census.CENSUS_Y > 1000, 'CENSUS_Y'] = 40.799935
        return census

    def set_cd(self, cd):
        return cd[['OBJECTID', 'LONG', 'LAT']]

    def set_disambiguated(self, disambiguated):
        self.disambiguated = disambiguated

    def set_confidence(self, confidence):
        self.confidence = confidence
        # Need to figure out what's going on here
        self.benchmark['confidence_score'] = self.benchmark[self.confidence] + self.benchmark['add_match']

    def create_benchmark(self):

        benchmark = self.match.merge(self.census, how='left', on='CENSUS_MATCH_ADDR', validate='many_to_one')

        benchmark['cd_hn'] = benchmark.apply(lambda row: get_hn(row.MATCH_ADDR), axis=1)
        benchmark['cen_hn'] = benchmark.apply(lambda row: get_hn(row.CENSUS_MATCH_ADDR), axis=1)
        benchmark['cd_add_cln'] = benchmark.apply(lambda row: get_st(row.MATCH_ADDR), axis=1)
        benchmark['cen_add_cln'] = benchmark.apply(lambda row: get_st(row.CENSUS_MATCH_ADDR), axis=1)

        benchmark['add_match'] = np.where(benchmark.cd_hn == benchmark.cen_hn, 0.1, 0) + np.where(
            benchmark.cen_add_cln == benchmark.cd_add_cln, 0.9, 0)

        benchmark.merge(self.cd, how='left', on='OBJECTID', validate='many_to_one')

        return benchmark

    def run_benchmarking(self):
        if self.confidence is None:
            raise Exception("Please set confidence first")
        if self.disambiguated is None:
            raise Exception("Please set diambiguated first")

        self.benchmark_results = da.get_dist_based_match(self.benchmark)['results']
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

#Static methods

def get_hn(add):
    hn = re.search('[0-9]+', add)
    return hn.group()

def get_st(add):
    try:
        st = re.search('(?<=[0-9]\\s)([A-Z]|\\s)+(?=,)', add)
        return st.group()
    except:
        return None


