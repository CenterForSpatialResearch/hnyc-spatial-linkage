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
  a. Generate a confidence score based on occupation (having occupation), age > 12, JW dist of name, relative probability (number of non-unique matches)  

### 1850
Similar, but no ED and address data, only ward data in the census.

## Analysis report (till now)

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

