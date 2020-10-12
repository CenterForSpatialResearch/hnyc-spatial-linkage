import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

import pandas as pd
from disambiguation import Disambiguator
import disambiguation.confidence_score_tuning as cf
import disambiguation.processing as dp


elastic_search_file = "../data/es-1850-22-9-2020.csv"
match_file = "../data/1850_mn_match_v2.csv"

elastic_match = pd.read_csv(elastic_search_file, sep='\t', engine='python')

elastic_match = dp.col_for_disamb(elastic_match, cd_id = "CD_RECORD_ID",cen_id = "CENSUS_IPUMS_UID")
elastic_match.loc[:,"confidence_score"] = cf.confidence_score(elastic_match, ['jw_score','cd_count_inverse','census_count_inverse', 'occ_listed', 'age_score'], [0.7,0.1,0.1,0.05,0.05])

print ("No. of matches: " + str(len(elastic_match)))
print ("No. of unique CD records: " + str(len(elastic_match['OBJECTID'].drop_duplicates())))
print ("No. of unique Census records: " + str(len(elastic_match['CENSUS_IPUMS_UID'].drop_duplicates())))
print ("No. of 1:1 matches: " + str(len(elastic_match[ (elastic_match['census_count'] == 1) & (elastic_match['cd_count'] == 1) ] )) )
print ("No. of matches where census record <= 12: " + str( len(elastic_match[elastic_match['CENSUS_AGE'] <= 12]) ))
print ("No. of unique matches where census record <= 12: " + str( len(elastic_match[elastic_match['CENSUS_AGE'] <= 12]['CENSUS_IPUMS_UID'].drop_duplicates()) ))
print ("No. of anchors (confidence score = 1): " + str( len(elastic_match[elastic_match['confidence_score'] == 1]) ))

disambiguate = Disambiguator(elastic_match, lon='CD_X', lat='CD_Y',sort_var='CENSUS_INDEX')
disambiguate.run_disambiguation()

result = disambiguate.get_result()

print("records with a final match:", sum(result.selected))
print("all records matched:", len(result))
print("number of cd records matched:", len(result['CD_ID'].drop_duplicates()))
print("proportion of cd records in elastic search included in final match:", sum(result.selected) / len(result['CD_ID'].drop_duplicates()))

result.to_csv(match_file, index=False)

