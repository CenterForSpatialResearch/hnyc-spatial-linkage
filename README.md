# HNYC Spatial Research on Spatial Linkages


1. Install elasticsearch from: https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html
2. Once elasticsearch is installed, make sure the elastic cluster is running. To run in Linux/Unix (Mac): `./bin/elasticsearch` in the elasticsearch extracted folder.
3. Clone repository
4. Go to repository root folder and run `pip install -r requirements.txt` to install dependencies.

To run scripts:
1. Data ingestion for 1880 census data: `python data_ingestion.py -config ../doc/config_1880.json`
2. Data ingestion for 1850 census data: `python data_ingestion.py -config ../doc/config_1850.json`
3. Match data for 1880: `python match_1880.py`
4. Match data for 1850: `python match_1850.py

## Summary of Processes Taken in Fall 2019
### 1880 Process
Two sources:  
- **City Directory**: name (first, last, initial), address, ID, occupation, ED, ward, block number (constructed)  
- **Census**: address (hidden during match process), ID (different from CD), occupation, ED, ward, age, gender, dwelling, household characteristics

Steps:  
0. Preprocessing: changing names to their phonetic index  
1. Initial entity link (to generate possible matches): criteria = same ED + JW dist < 2 on indexed name  
2. Disambiguation (to choose between non-unique matches):  
  a. Generate a confidence score based on occupation (having occupation), age > 12 (**not implemented**), JW dist of name, relative probability (number of non-unique matches)  

### 1850
Similar, but no ED and address data, only ward data in the census.

## Matching using ES
### Analysis report (till now)

<b> Task: Link the city directory entries with the Census data of 1880. </b>
  
* There are two sample datasets: a) City directory, b) Census data of 1880
* The total entries in the City directory are: 31,601

<b> Methodology: </b>
* First level of matching was done to find direct matches between the First Name, Last Name, Enumeration District (ED), Ward Num (WN). Here, no fuzzy matching, nor use of the geo-location was used. `Total Matches: 8,741. Unmatched: 22,860`

* Second level of matching was done on the unmatched dataset using fuzzy and geo-location to disambiguate:
  * Match only on names with no ED, WN or fuzzy or geolocation match. `Total Match (name_match) : 4,920 and Unmatched (name_unmatch): 17,941`
  * name_unmatch were matched using Edit Distance of 2 and geolocation matching within 1 km radius. `Total Match (fuzzy_name_match): 9,716`
  * name_match were matched again but this time we included ED but no fuzzy-matching. `Total Unmatch (ed_mismatch): 4919`

* Additional testing was to match using edit distance of 2 and geolocation radius of 1km. `Unmatched: 10,371`

<b> Findings: </b>
* EDs and WD can be faulty for some addresses.
* People can also have their middle names. 17 in total in the sample.
* Names can be written wrong.

<b> Conclusion: </b>
* Total matches with high confidence: 8,741 + 9,716 = 18,457
* Total matches with medium confidence: 4,920
* Total matches: 23,377/31,601 = 73.97%

<b> Future Work: </b>
* Match name_match with geolocation and no ED, WD
* Match on addresses
* Create a unique hash of the census data

### ES queries:

* Address disambiguation 1880:
`{ "bool" : { "must" : 
            [{ "fuzzy": { "CENSUS_NAMEFRSTB": { "value": data[0], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
            {"fuzzy": { "CENSUS_NAMELASTB": { "value": data[1], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
            { "geo_distance" : { "distance" : "1km", "LOCATION": { "lat" : round(float(data[-2]),2), "lon" : round(float(data[-1]),2)} } }
            ]}}`
           
* Address disambiguation 1850:
` { "bool": { "must": [{ "fuzzy": { "CENSUS_NAMEFRST": { "value": data['CD_FIRST_NAME'], "fuzziness": "AUTO", "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" }}}, 
                        {"fuzzy": { "CENSUS_NAMELAST": { "value": data['CD_LAST_NAME'], "fuzziness": "AUTO", "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" }} },
                        {"range": {"CENSUS_WARD_NUM": {"gte": int(data["WARD_NUM"])-1, "lte": int(data["WARD_NUM"])+1}}}
                        ]}}`
 
* Direct match 1880: 
` { "bool" : { "must" : [{ "match" : { "CENSUS_NAMEFRSTB" : data["CD_FIRST_NAME"] } },
                { "match" : { "CENSUS_NAMELASTB" : data["CD_LAST_NAME"] } },
                { "match" : { "WARD_NUM": data["WARD_NUM"]} },
                { "match" : { "CENSUS_ED": data["CD_ED"]} }] } }`

## Disambiguation
### Description of scripts:
1. **preprocess.ipynb** - Reads the source files - [cd_1880_mn_v04.csv](https://drive.google.com/open?id=1jfTtmmBLtWpJydUI2nRJQYXrzDHx8K-q), [census_1880_mn_v04.csv](https://drive.google.com/open?id=11jpmKMhbB0waX7vwwn5_5sBu4nBfvJ-F) and prepares corresponding files - `cd_df_final.csv` and `census_df_final.csv` to run the code to link records on.
2. **run_link_records.ipynb** - Reads `cd_df_final.csv` and `census_df_final.csv` and runs code to link the records using spark. Spark is preferred due to the large computation required when doing the joins.

Match results are contained in the file `cd_census_merged.csv`

3. **disambiguation_analysis.ipynb** - experiment with Jaro-Winkler distance between the names and other information recorded for the individuals to get a condifence score for the matches. 

#### Calculation of the confidence score, as implemented in script 3:

    0.5 x (jaro-winkler score) + 0.35 x (1/number of conflicts) + 0.15 x (1 if census occupation listed else 0)

where, *jaro-winkler score* = .4 x (jaro-winkler score of first names) + .6 x (jaro-winkler score of last names)


#### The final results with the confidence score are in the file `match_results_confidence_score.csv`


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


#### Deomonstration of obtaining confidence score on the output of Elastic Search (implemented by Amogh Mishra @AmoghM):

- `test.csv` is a sample output from the elastic search based matching process
- **get_confidence_score.ipynb** reads the file `test.csv` and adds a column with the confidence score of the matches
