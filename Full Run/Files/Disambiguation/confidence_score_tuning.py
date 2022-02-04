from __init_disambiguation__ import Disambiguator, Disambiguator1880, Benchmark, Benchmark_v02

"""
Purpose: Generate confidence scores as a list
df: elastic search dataframe formatted for disambiguation
columns: columns we want to use to create confidence score
weights: corresponding weights we want to use to create confidence score, should sum to one
"""
def confidence_score(df, columns, weights):
    return [sum(row[col]*w for col, w in zip(columns,weights)) for index,row in df.iterrows()]

"""
#Unneeded in new data run
Purpose: Format census data for benchmarking
census: census data 
"""
def census_for_disamb(census):
    census_latlng_tuning = census.copy()
    census_latlng_tuning['CENSUS_ID'] = 'CENSUS_' + census_latlng_tuning['OBJECTID.x'].astype(str)
    census_latlng_tuning = census_latlng_tuning.loc[:, ['CENSUS_ID', 'CENSUS_X', 'CENSUS_Y']]
    census_latlng_tuning.loc[census_latlng_tuning.CENSUS_Y > 1000, 'CENSUS_Y'] = 40.799935
    return census_latlng_tuning

"""
param_grid: list of dictionaries with names of columns to use for a trial cf score and corresponding weights
df_allcols: elastic search output formatted for disambiguation
df_census: census data for benchmarking
df_cd: city directory data for benchmarking
"""
def confidence_score_tuning(param_grid, df_allcols, df_census, df_cd):
    # Store results
    results = {}
    df = df_allcols.copy()

    # Get confidence score for each value in grid
    for i in range(len(param_grid)):
        name = "confidence_score_" + str(i)
        df.loc[:, name] = confidence_score(df_allcols, param_grid[i]["columns"], param_grid[i]["weights"])

    # Create benchmark object
    benchmark = Benchmark(df, df_census, df_cd)

    # Format census data for tuning
    census_tuning = census_for_disamb(df_census)

    # try:
    for i in range(len(param_grid)):
        name = "confidence_score_" + str(i)

        # Run disambiguation process (use betweeness and clustering -- based on Jolene's work)
        basic = Disambiguator1880(df, confidence=name)

        try:
            basic.run_disambiguation()
        except:
            continue

        result = basic.get_result()  # .to_csv("..data/confidence_score_tuning/confidence_score_"+str(i))

        # Results analysis
        basic.merge_census_var(census_tuning)
        basic.set_var()

        # benchmarking
        benchmark.set_confidence(name)
        benchmark.set_disambiguated(result)
        benchmark.run_benchmarking()

        # Store results
        results[name] = {"columns": param_grid[i]["columns"], "weights": param_grid[i]["weights"],
                         "Match Rate": basic.get_match_rate(), "Address Success": basic.get_addr_success(),
                         "Under 12": basic.get_under12_selections(),
                         "confusion matrix": benchmark.get_confusion_matrix()}

        # will return results so far even if exception occurs
        # Spit out the best columns and weights (Add this in when decide what makes something the best)
        # For now simply output the analysis
    return results

#Uses new version of benchmarking, bc elastic search output means we don't need to join in the x/ys separately
def confidence_score_tuning_v02(param_grid, df_elastic_search):
    # Store results
    results = {}
    df = df_elastic_search.copy()

    # Get confidence score for each value in grid
    for i in range(len(param_grid)):
        name = "confidence_score_" + str(i)
        df.loc[:, name] = confidence_score(df_elastic_search, param_grid[i]["columns"], param_grid[i]["weights"])

    benchmark = Benchmark_v02(df_elastic_search)

    for i in range(len(param_grid)):
        name = "confidence_score_" + str(i)

        # Run disambiguation process (use betweeness and clustering -- based on Jolene's work)
        basic = Disambiguator1880(df, confidence=name)

        #try:
        basic.run_disambiguation()
        #except:
            #continue

        result = basic.get_result()  # .to_csv("..data/confidence_score_tuning/confidence_score_"+str(i))

        # Results analysis
        basic.set_var()

        # benchmarking
        benchmark.set_confidence(name)
        benchmark.set_disambiguated(result)
        benchmark.run_benchmarking()

        # Store results
        results[name] = {"columns": param_grid[i]["columns"], "weights": param_grid[i]["weights"],
                         "Match Rate": basic.get_match_rate(), "Address Success": basic.get_addr_success(),
                         "Under 12": basic.get_under12_selections(),
                         "confusion matrix": benchmark.get_confusion_matrix()}

        # will return results so far even if exception occurs
        # Spit out the best columns and weights (Add this in when decide what makes something the best)
        # For now simply output the analysis
    return results
