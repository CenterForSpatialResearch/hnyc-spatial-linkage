from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json

es = Elasticsearch('localhost:9200')
def match_addr():
    # df = pd.read_csv('../data/cd1880_sample_LES_v04.csv')
    df = pd.read_csv('../data/cd_1850_mn_v04.csv')
    bulk_data = []
    clean_data = {}
    count, match,unmatch = 0,0,0
    with open('../data/address.csv','w') as fw, open('../data/address_notmatch.csv','w') as fw2:
        for index, row in df.iterrows():
            row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
            data = row.to_dict()
            try:
                query = { "bool": { "must": [{ "fuzzy": { "CENSUS_NAMEFRST": { "value": data['CD_FIRST_NAME'], "fuzziness": "AUTO", "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" }}}, 
                        {"fuzzy": { "CENSUS_NAMELAST": { "value": data['CD_LAST_NAME'], "fuzziness": "AUTO", "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" }} },
                        {"range": {"CENSUS_WARD_NUM": {"gte": int(data["WARD_NUM"])-1, "lte": int(data["WARD_NUM"])+1}}}
                        ]}}
            except ValueError:
                pass
                count+=1
            # { "match_phrase" : { "MATCH_ADDR" : data["MATCH_ADDR"] } }
            # {"range": {"WARD_NUM": {"gte": data["WARD_NUM"]-1, "lte": data["WARD_NUM"]+1}}},
            #{"range": {"CENSUS_ED": {"gte": data["CD_ED"]-1, "lte": data["CD_ED"]+1}}}
            # try:
            res = es.search(index="census-1850", body={ "from": 0, "size": 5, "query":query})
            if res['hits']['total']['value']!= 0:
                match+=1
                for i in res['hits']['hits']:
                    score = str(i['_score'])
                    i = i['_source']
                    wr = i['CENSUS_IPUMS_UID']+","+i['CENSUS_NAMEFRST']+","+i['CENSUS_NAMELAST']+","+str(i['CENSUS_WARD_NUM'])+","+str(data['OBJECTID'])+","+data['CD_FIRST_NAME']+","+data['CD_LAST_NAME']+","+str(data['CD_RECORD_ID'])+","+data['CD_ADDRESS']+","+score
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