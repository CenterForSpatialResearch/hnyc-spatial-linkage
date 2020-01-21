# hnyc-spatial-linkage

### Description of scripts:
1. **preprocess.ipynb** - Reads the source files - [cd_1880_mn_v04.csv](https://drive.google.com/open?id=1jfTtmmBLtWpJydUI2nRJQYXrzDHx8K-q), [census_1880_mn_v04.csv](https://drive.google.com/open?id=11jpmKMhbB0waX7vwwn5_5sBu4nBfvJ-F) and prepares corresponding files - `cd_df_final.csv` and `census_df_final.csv` to run the code to link records on.
2. **run_link_records.ipynb** - Reads `cd_df_final.csv` and `census_df_final.csv` and runs code to link the records using spark. Spark is preferred due to the large computation required when doing the joins.

Match results are contained in the file `cd_census_merged.csv`

3. **disambiguation_analysis.ipynb** - experiment with Jaro-Winkler distance between the names and other information recorded for the individuals to get a condifence score for the matches. 

The final results with the confidence score are in the file `match_results_confidence_score.csv`



| | Observation |
| -------------: |-----|
| 1,116,598 | Number of records in the census directory (duplicates removed)|
| 222,618 | Number of records in the city directory (duplicates removed)| 
| 138,242 | Number of potential matches |
| 105,091 | Number of city directory records with potential matches |
| 132,420 | Number of census records involved in potential matches |
| 77,849 | Number of city directory records with unique potential match |
| 27,242 | Number of city directory records with more than one potential match |
| 1,898 | Number of unique matches where **AGE** in CENSUS record is **12 years or lesser** |
| 15,612 | Number of *non*-unique matches where **AGE** in CENSUS record is **12 years or lesser** |
