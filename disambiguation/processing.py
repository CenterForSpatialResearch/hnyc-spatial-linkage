import pandas as pd
import networkx as nx
import numpy as np

"""
Create a dictionary, with keys as group ID and values as a dataframe bounded by anchors
Each dataframe is one where spatial disambiguation will be applied
This is necessary as else, algorithms take too long to run
Match: df of matches
confidence_score: name of confidence score column
"""

def create_subdict(match, confidence="confidence_score"):
    
    # identify anchors and assign anchor ID
    match['anchor'] = np.where(match[confidence] == 1, 1, None)
    sub_group = pd.DataFrame({'index': list(match.loc[match.anchor.notnull(), :].index), 'group_ID': range(0, sum(match['anchor'].notnull()))}).set_index('index')
    match = match.join(sub_group)
    match['group_ID'] = match['group_ID'].fillna(method='ffill').fillna(method='backfill')

    # split df into multiple df, each bounded by anchor
    sub_group_dict = {group: df for group, df in match.groupby('group_ID')}

    # add bottom anchor back
    for i in range(0, len(sub_group_dict) - 1):
        sub_group_dict[i] = pd.concat([sub_group_dict[i], sub_group_dict[i+1][0:1]])

    return sub_group_dict

"""
Create node ID for each match, to be using the shortest path algorithm 
sub must be a df with each row as a potential match between a CD and census record. It must contain the columns CD_ID, CENSUS_ID, LONG, LAT, confidence_score and MATCH_ADDR.
the column names can be specified individually if they are named differently
Returns the dataframe with new columns, 'anchor', 'node_ID' and 'letter'.
anchor: whether row is an anchor (confidence score = 1)
node_ID: unique node ID. each node is a match, so e.g. A0 and A1 refers to two potential CD matches for the same census record
letter: grouping for identical census records 
add_prefixes: whether to add prefixes 'CD_' and 'CENSUS_' to cd_id and census_id respectively. prefixes are required for subsequent bipartite matching
"""
def create_path_df(sub_graph, census_id = "CENSUS_ID", confidence = "confidence_score"):
    
    sub_graph['node_ID'] = sub_graph.groupby(census_id).cumcount()

    letter_id = sub_graph[census_id].unique().tolist()
    letters = ['N' + str(x) for x in range(0, len(letter_id))]
    letter_id = pd.DataFrame({'CENSUS_ID': letter_id, 'letter': letters})

    sub_graph = sub_graph.merge(letter_id, how='left', left_on=census_id, right_on="CENSUS_ID", validate='many_to_one')

    sub_graph['node_ID'] = sub_graph.apply(lambda row: row.letter + '_' + str(row.node_ID), axis=1)
    
    return sub_graph

"""
Creates a graph from the sub_graph dataframe
Each node being a potential CD-census match and 
    each edge being the link between the potential CD records of consecutive census records
The weight of each edge = the manhattan distance between the two
cluster_col: name of column with cluster group. If does not exist, use None
Returns the graph object
"""

def create_path_graph(g, cluster_col='in_cluster_x'):
    g.loc[:, 'key'] = 0
    g = g.merge(g, on='key')
    g['key'] = g.apply(lambda row: 1 if int(row.letter_x[1:]) - int(row.letter_y[1:]) == -1 else 0, axis = 1)
    g = g[g.key == 1]

    g['weight'] = g.apply(lambda row: ((row.LONG_y - row.LONG_x)**2 + (row.LAT_y - row.LONG_x)**2)**(1/2), axis=1)

    if cluster_col != None:
        g['weight'] = g.apply(lambda row: row.weight + 999 if row[cluster_col] == -1 else row.weight, axis=1)
    
    g_edges = [(row.node_ID_x, row.node_ID_y, row.weight) for index, row in g.iterrows()]
    graph = nx.DiGraph()
    graph.add_weighted_edges_from(g_edges)
    
    return graph