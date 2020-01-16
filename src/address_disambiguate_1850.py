from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json
from metaphone import doublemetaphone

es = Elasticsearch('localhost:9200')
def match_addr():
    # df = pd.read_csv('../data/cd1880_sample_LES_v04.csv')
    df = pd.read_csv('../data/cd_1850_mn_v04.csv')
    bulk_data = []
    clean_data = {}
    count, match,unmatch = 0,0,0
    with open('../data/es-1850.csv','w') as fw, open('../data/es-1850-unmatch.csv','w') as fw2:
        fw.write('CENSUS_IPUMS_UID'+","+'CENSUS_NAMEFRST'+","+'CENSUS_NAMELAST'+","+'CENSUS_WARD_NUM'+","+'OBJECTID'+","+'CD_FIRST_NAME'+","+'CD_LAST_NAME'+","+'CD_ADDRESS'+","+'BLOCK_NUM'+","+'CENSUS_AGE'+","+'CENSUS_DWELLIN_NUM'+","+'CENSUS_DWELLLING_SEQ'+","+'CENSUS_OCCSTR'+","+"CD_OCCUPATION")
        for index, row in df.iterrows():
            row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
            data = row.to_dict()
            
            first_name_metaphone = [i for i in doublemetaphone(data["CD_FIRST_NAME"]) if i]
            last_name_metaphone = [i for i in doublemetaphone(data["CD_LAST_NAME"]) if i]

            # query = { "bool" : { "must" : [{ "match" : { "CENSUS_NAMEFRSTB" : data["CD_FIRST_NAME"] } },
            #         { "match" : { "CENSUS_NAMELASTB" : data["CD_LAST_NAME"] } },
            #         { "match" : { "WARD_NUM": data["WARD_NUM"]} },
            #         { "match" : { "CENSUS_ED": data["CD_ED"]} },
            #         {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
            #         {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}
            #         ],
            #         }}
            
            query = { "bool" : { "must" : [{ "fuzzy": { "CENSUS_NAMEFRST": { "value": data["CD_FIRST_NAME"], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
                    {"fuzzy": { "CENSUS_NAMELAST": { "value": data["CD_LAST_NAME"], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                    { "match" : { "CENSUS_WARD_NUM": data["WARD_NUM"]} },
                    {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
                    {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}]}}
            
            # try:
            #     query = { "bool": { "must": [{ "fuzzy": { "CENSUS_NAMEFRST": { "value": data['CD_FIRST_NAME'], "fuzziness": "AUTO", "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" }}}, 
            #             {"fuzzy": { "CENSUS_NAMELAST": { "value": data['CD_LAST_NAME'], "fuzziness": "AUTO", "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" }} },
            #             {"range": {"CENSUS_WARD_NUM": {"gte": int(data["WARD_NUM"])-1, "lte": int(data["WARD_NUM"])+1}}}
            #             ]}}
            # except ValueError:
            #     pass
            #     count+=1
            # { "match_phrase" : { "MATCH_ADDR" : data["MATCH_ADDR"] } }
            # {"range": {"WARD_NUM": {"gte": data["WARD_NUM"]-1, "lte": data["WARD_NUM"]+1}}},
            #{"range": {"CENSUS_ED": {"gte": data["CD_ED"]-1, "lte": data["CD_ED"]+1}}}
            # try:
            try:
                res = es.search(index="mn-census-1850", body={ "from": 0, "size": 10000, "query":query})
            except:
                print(index)
                continue

            if res['hits']['total']['value']!= 0:
                match+=1
                for i in res['hits']['hits']:
                    # score = str(i['_score'])
                    i = i['_source']
                    wr = i['CENSUS_IPUMS_UID']+","+i['CENSUS_NAMEFRST']+","+i['CENSUS_NAMELAST']+","+str(i['CENSUS_WARD_NUM'])+","+str(data['OBJECTID'])+","+data['CD_FIRST_NAME']+","+data['CD_LAST_NAME']+","+data['CD_ADDRESS']+","+ str(data['BLOCK_NUM'])+","+str(i['CENSUS_AGE'])+","+ str(i['CENSUS_DWELLIN_NUM'])+","+ str(i['CENSUS_DWELLING_SEQ'])+","+i['CENSUS_OCCSTR']+","+data['CD_OCCUPATION']
                    fw.write(wr+"\n")
                    
            else:
                fw2.write(str(data['OBJECTID'])+"\n")
                unmatch+=1
            # except KeyError:
            #     count+=1
            #     pass
    print(count,match,unmatch)

if __name__=='__main__':
    match_addr()