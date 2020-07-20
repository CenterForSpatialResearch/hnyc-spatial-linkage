from disambiguation import Disambiguator1880
from disambiguation.benchmark import Benchmark

def confidence_score(df, columns, weights):
    return [sum(row[col]*w for col, w in zip(columns,weights)) for index,row in df.iterrows()]


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
    census_tuning = census_for_disamb(census)

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
