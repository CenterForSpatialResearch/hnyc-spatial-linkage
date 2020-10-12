from haversine import haversine, Unit
from numpy import log
import disambiguation.disambiguation as dl

"""
Get number of selected matches, out of total possible (ie unique CD records)
df: df with "selected" column, after running get_matches()
cd_id: name of cd_id column
"""
def get_match_rate(df, cd_id='CD_ID'):
    n_cd_records = len(df[cd_id].unique())
    n_selected = sum(df["selected"].values)
    match_rate = round(n_selected / n_cd_records * 100, 2)

    return match_rate

"""
Get number of perfect matches (in terms of address) selected
df: df with "selected" column, after running get_matches()
cd_add: name of cd address column
cen_add: name of cen_add column
"""
def get_addr_success(df, cd_add='MATCH_ADDR', cen_add='CENSUS_MATCH_ADDR'):
    df['cd_add_cln'] = df.apply(lambda row: row[cd_add][:row[cd_add].index(',')], axis=1)
    df['cen_add_cln'] = df.apply(lambda row: row[cen_add][:row[cen_add].index(',')], axis=1)
    n_perfect_match_chosen = len(df.loc[(df['cd_add_cln'] == df['cen_add_cln']) & (df['selected'] == 1), :])
    n_perfect_match = len(df.loc[df['cd_add_cln'] == df['cen_add_cln'], :])

    return {'n_perfect_match_chosen': n_perfect_match_chosen, 'n_perfect_match': n_perfect_match}

"""
Get error rate based on distance (in metres) between matched and actual address
df: df with "selected" column, after running get_matches()
cen_lon: name of census long column
cen_lat: name of census lat column
lon: name of long column
lat: name of lat column
"""
def get_dist_error(df, cen_lon='CENSUS_X', cen_lat='CENSUS_Y', lon='CD_X', lat='CD_Y'):
    df['dist'] = df.apply(lambda row: haversine((row[cen_lat], row[cen_lon]), (row[lat], row[lon]), unit=Unit.METERS), axis=1)
    return df

"""
Get number of selected matches, out of total possible (ie unique CD records)
df: df with "selected" column, after running get_matches()
cd_id: name of cd_id column
"""
def get_under12_selections(df, age='CENSUS_AGE'):
    n_under12 = len(df.loc[(df[age] <= 12) & (df['selected'] == 1), :])
    n_selected = len(df.loc[df['selected'] == 1, :])
    proportion = round(n_under12 / n_selected * 100, 2)

    return proportion

"""
Get df containing selected matches based on actual distances and confidence score
df: any match df containing at least cd_id, census_id, census long/lat, cd long/lat and confidence score. preferably df with 'dist' column (after get_dist_error())
"""
def get_dist_based_match(df, cen_lon='CENSUS_X', cen_lat='CENSUS_Y', lon='CD_X', lat='CD_Y', cd_id='CD_ID', census_id='CENSUS_ID', confidence='confidence_score'):
    if 'dist' not in df.columns:
        df = get_dist_error(df, cen_lon=cen_lon, cen_lat=cen_lat, lon=lon, lat=lat)
    
    df['dist_weight'] = round(1 / log(df['dist']) + df[confidence], 2)
    dist_disamb = dl.get_matches(df, cd_id = cd_id, census_id = census_id, weight = 'dist_weight')

    return dist_disamb

"""
Get false positive and false negative rates
df_algo: df with "selected" column, after running get_matches()
df_dist: df with "selected" column, after running get_dist_based_match()
Returns confusion matrix and df_algo with 'selected' now called 'selected_algo', and an additional column, 'selected_dist' which indicates 'true' matches.
"""
def compare_selections(df_algo, df_dist, cd_id="CD_ID", census_id="CENSUS_ID"):
    df_algo = df_algo.merge(df_dist.loc[:, [cd_id, census_id, 'selected']], how="inner", on=[cd_id, census_id], validate='one_to_one', suffixes=('_algo', '_dist'))

    true_positive = len(df_algo.loc[(df_algo['selected_algo'] == 1) & (df_algo['selected_dist'] == 1), :])
    false_positive = len(df_algo.loc[(df_algo['selected_algo'] == 1) & (df_algo['selected_dist'] == 0), :])
    false_negative = len(df_algo.loc[(df_algo['selected_algo'] == 0) & (df_algo['selected_dist'] == 1), :])
    true_negative = len(df_algo.loc[(df_algo['selected_algo'] == 0) & (df_algo['selected_dist'] == 0), :])

    confusion_matrix = [[true_positive, false_positive], [false_negative, true_negative]]
    return {'confusion_matrix': confusion_matrix, 'merged_df': df_algo}