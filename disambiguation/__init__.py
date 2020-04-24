import pandas as pd
import disambiguation.processing as dp
import disambiguation.linkage as dl
import disambiguation.analysis as da

class Disambiguator:

    def __init__(self, match_df, cd_id="CD_ID", census_id="CENSUS_ID", confidence="confidence_score", lon="LONG", lat="LAT"):
        # initialize input 
        self.input = match_df

        # initialize col names
        self.cd_id = cd_id
        self.census_id = census_id
        self.confidence = confidence
        self.lon = lon
        self.lat = lat

        # output variables 
        self.bipartite = None
        self.results = None

    def run_disambiguation(self, cluster=True, k_between=True, cluster_kwargs = {}, path_kwargs = {}):
        print("Running")

        print("Creating dictionary of sub dfs (1/4)...")
        sub_group_dict = dp.create_subdict(self.input, self.confidence)
        sub_group_algos = [i for i in range(0, len(sub_group_dict) - 1) if sum(sub_group_dict[i].census_count > 1) > 0]

        print("Applying algorithms iteratively (2/4)...")
        # iteratively apply algorithms onto each sub df
        for i in sub_group_algos:
            
            # save df and wrangle to necessary format
            df = sub_group_dict[i]
            path_df = dp.create_path_df(df, self.census_id, self.confidence)

            if cluster:
                # apply density clustering and remove outlier nodes
                path_df = dl.apply_density_clustering(path_df, self.lat, self.lon, **cluster_kwargs)
                cluster_arg = 'in_cluster_x'

            else:
                cluster_arg = None

            # create graph and k shortest paths centrality
            g = dp.create_path_graph(path_df, cluster_col=cluster_arg)

            if k_between:
                output = dl.apply_k_betweenness(path_df, g, **path_kwargs)
            else:
                output = dl.apply_shortest_path(path_df, g, **path_kwargs)

            sub_group_dict[i] = output
        
        print("Cleaning output (3/4)...")
        sub_grp_list = [v for k,v in sub_group_dict.items()]
        final = pd.concat(sub_grp_list)

        final = final.drop_duplicates([self.cd_id, self.census_id])

        final['anchor'] = final['anchor'].fillna(0)
        final['spatial_weight'] = final.apply(lambda row: row[self.confidence] + 1 if pd.isna(row.spatial_weight) else row.spatial_weight, axis=1)

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
