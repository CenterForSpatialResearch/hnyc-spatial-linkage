import pandas as pd
import numpy as np
import disambiguation.processing as dp
import disambiguation.linkage as dl
import disambiguation.analysis as da

class Disambiguator:

    def __init__(self, match_df, cd_id="CD_ID", census_id="CENSUS_ID", confidence="confidence_score", census_count='census_count', lon="LONG", lat="LAT", sort_var="CENSUS_ID"):
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

    def set_var(self, var={'cen_lon': 'CENSUS_X', 'cen_lat': 'CENSUS_Y', 'cen_add': 'MATCH_ADDR', 'cd_add': 'CENSUS_MATCH_ADDR'}):
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
