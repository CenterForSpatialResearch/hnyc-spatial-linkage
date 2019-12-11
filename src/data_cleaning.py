from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json

es = Elasticsearch('localhost:9200')

def get_matches(path):
    df = pd.read_csv('../data/cd1880_sample_LES_v04.csv')
    bulk_data = []
    clean_data = {}
    for index, row in df.iterrows():
        row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
        data = row.to_dict()
        query = { "bool" : { "must" : [{ "match" : { "CENSUS_NAMEFRSTB" : data["CD_FIRST_NAME"] } },
                { "match" : { "CENSUS_NAMELASTB" : data["CD_LAST_NAME"] } },
                { "match" : { "WARD_NUM": data["WARD_NUM"]} },
                { "match" : { "CENSUS_ED": data["CD_ED"]} }] } } 
        res = es.search(index="sample-census", body={ "from": 0, "size": 1, "query":query})
        if res['hits']['total']['value']!= 0:
            clean_data[data['OBJECTID']] = res['hits']['hits'][0]['_source']['CENSUS_MERGEID']
        else:
            print(data["CD_FIRST_NAME"],",",data["CD_LAST_NAME"],",",data["WARD_NUM"],",",data["CD_ED"],",",data["LAT"],",",data["LONG"])
    return clean_data

def export_data(data):
    json.dump(data, open('../data/matched_data.json','w'))

if __name__=='__main__':
    clean_data = get_matches('../data/cd1880_sample_LES_v04.csv')           
    # export_data(clean_data)