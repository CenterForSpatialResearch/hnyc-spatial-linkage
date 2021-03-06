# Analysis report (Fall 2019)

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

## ES queries:

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

