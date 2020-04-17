import pandas as pd
import networkx as nx
import hdbscan

"""
Apply Dijkstra's algorithm to the graph and get spatial weights
Spatial weights are computed as confidence score +1 if match was included in shortest path, and confidence score + 0 otherwise
df: dataframe of records with confidence score and node ID, names can be modified via parameters
graph: graph object created from create_path_graph()
source: start node, e.g. 'A0'. By default it chooses first row in the table
target: end node, e.g. 'J0'. By default it chooses last row in the table
Returns a dictionary of:
    'df': dataframe of matches with added 'spatial weight'
    'shortest_path': list containing nodes in the shortest path
"""
def apply_shortest_path(df, graph, source = None, target = None, confidence = 'confidence_score', node_id = 'node_ID'):
    if source == None:
        source = list(df[node_id])[0]
    if target == None:
        target = list(df[node_id])[-1]

    path = nx.dijkstra_path(graph, source, target)
    df['spatial_weight'] = df.apply(lambda row: row[confidence] + 1 if row[node_id] in path else row[confidence], axis = 1)

    return {'df': df, 'shortest_path': path}

"""
Apply betweenness centrality using k shortest paths (as documented in spatial_disambiguation.ipynb)
df: df of matches
graph: graph object created from create_path_graph()
source: start node, e.g. 'A0'. By default it chooses first row in the table
target: end node, e.g. 'J0'. By default it chooses last row in the table
k: how many shortest paths to choose from (absolute number). By default ~ 1/2 of number of possible paths
scale: how much to scale the score by when adding it with confidence score. Default = 1 (equal weight of confidence score and spatial weight)
Returns
    df: df with spatial weights column
    weights: list of between centrality weights
"""
def apply_k_betweenness(df, graph, source=None, target=None, weight="weight", k=None, scale=1, node_id = 'node_ID'):
    if source == None:
        source = list(df[node_id])[0]
    if target == None:
        target = list(df[node_id])[-1]

    k_paths = list(nx.shortest_simple_paths(graph, source, target, weight=weight))
    length = len(k_paths)

    if k == None:    
        if length < 31:
            k = 1
        else:
            k = int(0.5 * length)
    k_paths = k_paths[0:k]

    # initialize output: dict with nodes as keys
    spatial_weights = dict.fromkeys(graph.nodes, 0)
    
    # count
    for path in k_paths:
        for node in path:
            spatial_weights[node] += 1
    
    spatial_weights = [[key , round(value / k, 2) * scale] for key, value in spatial_weights.items()]
    spatial_df = pd.DataFrame(spatial_weights, columns=[node_id, 'spatial_weight'])
    df = df.merge(spatial_df, how="inner", on=node_id, validate="one_to_one")
    df['spatial_weight'] = df['spatial_weight'] + df['confidence_score']

    return {'df': df, 'weights': spatial_weights, 'paths': k_paths}

"""
Apply density based clustering to detect outliers. Requires `hdbscan` library
Refer to hdbscan documentation on parameters
Returns df with a column 'in_cluster' indicating which cluster the nodes are in
"""
def apply_density_clustering(df, min_cluster_size=10, min_samples=10, allow_single_cluster=True, lat='LAT', lon="LONG"):
    cluster_sub = df.loc[:, [lon, lat]]
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples, allow_single_cluster=allow_single_cluster).fit(cluster_sub)

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
def get_matches(sub_graph, cd_id = 'CD_ID', census_id = 'CENSUS_ID', weight = 'spatial_weight'):
    b_edges = [(row[cd_id], row[census_id], row[weight]) for index, row in sub_graph.iterrows()]
    b = nx.Graph()
    b.add_weighted_edges_from(b_edges)

    # algorithm is too expensive if we perform it on entire graph. moreover, graph is actually disconnected into sub_graphs. apply algorithm on subgraphs instead
    subgraphs = list(nx.connected_component_subgraphs(b))
    matches = [list(nx.max_weight_matching(graph, maxcardinality = True)) for graph in subgraphs]
    matches = [sorted(list(item)) for sublist in matches for item in sublist] # unnest and convert pairs from tuple to list
    matches = pd.DataFrame(matches, columns=[cd_id, census_id])
    matches['selected'] = 1

    sub_graph = sub_graph.merge(matches, how='left', on=[cd_id, census_id], validate='one_to_one')
    sub_graph['selected'] = sub_graph['selected'].fillna(0)

    # add subgraph id
    for i in range(0, len(subgraphs)):
        nodes = list(subgraphs[i].nodes)
        for node in nodes:
            if node[:2] == 'CD':
                final_processed.at[final_processed.CD_ID == node, 'graph_ID'] = i

    return {'graph': subgraphs, 'results': sub_graph}
