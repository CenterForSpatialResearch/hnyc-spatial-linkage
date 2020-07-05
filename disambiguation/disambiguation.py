import pandas as pd
import networkx as nx
import hdbscan
from itertools import islice
import disambiguation.processing as dp

"""
Wrapper function for everything below, including checks
Designed to work within list comprehension only! (refer to Disambiguator())
Works by applying algorithm to specified df (using index i) in the list
sub_groups: list of dfs, each df being a subset of the census bounded by 2 anchors
i: index of df in the list
"""
def apply_algo(sub_groups, i, cluster=True, k_between=True, census_id='CENSUS_ID', census_count="census_count", confidence='confidence_score', lat="LAT", lon="LON", cluster_kwargs={}, path_kwargs={}):

    if i % 1000 == 0:
        print("Reached: " + str(i))
    df = sub_groups[i]
    if sum(df[census_count] > 1) == 0: # no disambiguation needed
        return df

    if i + 1 < len(sub_groups): # add bottom anchor
        df = pd.concat([df, sub_groups[i+1][0:1]]) 
    
    path_df = dp.create_path_df(df, census_id, confidence)

    if cluster:
        # apply density clustering and remove outlier nodes
        path_df = apply_density_clustering(path_df, lat, lon, **cluster_kwargs)
        cluster_arg = 'in_cluster_x'

    else:
        cluster_arg = None

    # create graph and k shortest paths centrality
    g = dp.create_path_graph(path_df, cluster_col=cluster_arg, lat=lat, lon=lon)

    if k_between:
        output = apply_k_betweenness(path_df, g, **path_kwargs)
    else:
        output = apply_shortest_path(path_df, g, **path_kwargs)

    return output

"""
Apply Dijkstra's algorithm to the graph and get spatial weights
Spatial weights are computed as confidence score +1 if match was included in shortest path, and confidence score + 0 otherwise
df: dataframe of records with confidence score and node ID, names can be modified via parameters
graph: graph object created from create_path_graph()
source: start node, e.g. 'A0'. By default it chooses first row in the table
target: end node, e.g. 'J0'. By default it chooses last row in the table
Returns a dataframe of matches with added 'spatial weight'
"""
def apply_shortest_path(df, graph, source = None, target = None, confidence = 'confidence_score', node_id = 'node_ID'):
    if source == None:
        source = list(df[node_id])[0]
    if target == None:
        target = list(df[node_id])[-1]

    path = nx.dijkstra_path(graph, source, target)
    df['spatial_weight'] = df.apply(lambda row: row[confidence] + 1 if row[node_id] in path else row[confidence], axis = 1)

    return df

"""
Apply betweenness centrality using k shortest paths (as documented in spatial_disambiguation.ipynb)
df: df of matches
graph: graph object created from create_path_graph()
source: start node, e.g. 'A0'. By default it chooses first row in the table
target: end node, e.g. 'J0'. By default it chooses last row in the table
k: how many shortest paths to choose from (absolute number). By default 1 or ~ 1/2 of number of possible paths if there are more than 30 paths
scale: how much to scale the score by when adding it with confidence score. Default = 1 (equal weight of confidence score and spatial weight)
Returns
    df: df with spatial weights column
    k_paths: paths used for calculation
"""
def apply_k_betweenness(df, graph, source=None, target=None, k=None, scale=1):
    if source == None:
        source = list(df["node_ID"])[0]
    if target == None:
        target = list(df["node_ID"])[-1]

    k_paths = nx.shortest_simple_paths(graph, source, target, weight="weight")

    length = get_n_paths(df)
    if k == None:
        if length < 31:
            k = 1
        elif length > 50:
            k = 50
        else:
            k = int(0.5 * length)

    k_paths = list(islice(k_paths, k))

    # initialize output: dict with nodes as keys
    spatial_weights = dict.fromkeys(graph.nodes, 0)
    
    # count
    for path in k_paths:
        for node in path:
            spatial_weights[node] += 1
    
    spatial_weights = [[key , round(value / k, 2) * scale] for key, value in spatial_weights.items()]
    spatial_df = pd.DataFrame(spatial_weights, columns=["node_ID", 'spatial_weight'])
    df = df.merge(spatial_df, how="inner", on="node_ID", validate="one_to_one")
    df['spatial_weight'] = df['spatial_weight'] + df['confidence_score']

    return df

"""
Helper method to count the number of possible paths in the graph
"""
def get_n_paths(df):
    k = 1
    counts = df.groupby('letter')['letter'].size().to_list()
    for count in counts:
        k *= count
    
    return k

"""
Apply density based clustering to detect outliers. Requires `hdbscan` library
Refer to hdbscan documentation on parameters
Returns df with a column 'in_cluster' indicating which cluster the nodes are in
"""
def apply_density_clustering(df, lat='LAT', lon="LONG", min_cluster_size=10, min_samples=10, allow_single_cluster=True, **kwargs):
    cluster_sub = df.loc[:, [lon, lat]]
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples, allow_single_cluster=allow_single_cluster, **kwargs).fit(cluster_sub)

    df['in_cluster'] = pd.Series(clusterer.labels_).values

    return df

"""
A bipartite graph is created from the matches, with each node being either a census or CD record and each edge indicating a potential match. 
Note that subgraph MUST have prefixes on the cd_id ('CD_') and census_id ('CENSUS_') columns
The matching algorithm (maximum weighted matching) will 
    (1) select sets of matches that give the highest number of matches 
    (2) choose the match set that has the highest weight based on that
Returns a dictionary with 'graph' as the list of bipartite graphs and 'results' being the original df with an additional 'selected' column, indicating the correct match and 'graph_id' column, indicated subgraph.
"""
def get_matches(df, cd_id = 'CD_ID', census_id = 'CENSUS_ID', weight = 'spatial_weight'):
    b_edges = [(row[cd_id], row[census_id], row[weight]) for index, row in df.iterrows()]
    b = nx.Graph()
    b.add_weighted_edges_from(b_edges)

    # algorithm is too expensive if we perform it on entire graph. moreover, graph is actually disconnected into sub_graphs. apply algorithm on subgraphs instead
    subgraphs = [b.subgraph(c) for c in nx.connected_components(b)]
    matches = [list(nx.max_weight_matching(graph, maxcardinality = True)) for graph in subgraphs]
    matches = [sorted(list(item)) for sublist in matches for item in sublist] # unnest and convert pairs from tuple to list
    matches = pd.DataFrame(matches, columns=[cd_id, census_id])
    matches['selected'] = 1

    df = df.merge(matches, how='left', on=[cd_id, census_id], validate='one_to_one')
    df['selected'] = df['selected'].fillna(0)

    # add subgraph id
    subgraph_id = [{'graph_ID': i, 'CD_ID': node} for i in range(0, len(subgraphs)) for node in list(subgraphs[i].nodes) if node[:2] == 'CD']
    subgraph_id = pd.DataFrame(subgraph_id)
    df = df.merge(subgraph_id, how="inner", left_on=cd_id, right_on="CD_ID", validate="many_to_one")

    return {'graph': subgraphs, 'results': df}