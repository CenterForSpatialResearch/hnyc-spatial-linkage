from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json

es = Elasticsearch('localhost:9200')

def find_total_umatch():
    count = 0
    with open("../data/total_mismatch.txt","w") as fw, \
        open("../data/unmatch.txt","r") as fr:
        for row in fr:
            row = row.replace(" ","")
            row = row.replace("\n","")
            data = row.split(",")
            query = { "bool" : { "must" : 
                    [{ "fuzzy": { "CENSUS_NAMEFRSTB": { "value": data[0], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
                    {"fuzzy": { "CENSUS_NAMELASTB": { "value": data[1], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                    { "geo_distance" : { "distance" : "1km", "LOCATION": { "lat" : round(float(data[-2]),2), "lon" : round(float(data[-1]),2)} } },
                    { "match" : { "WARD_NUM": data[2]} }
                    ]}}
            try:
                res = es.search(index="sample-census", body={ "from": 0, "size": 1, "query":query})
                if res['hits']['total']['value']== 0:
                    fw.write(row+"\n")
                    
            except:
                count+=1
                pass
    print(count)

def read_file():
    with open('../data/unmatch.txt','r') as f, \
        open('../data/name_mismatch.txt','w') as fw, \
        open('../data/ed_mismatch.txt','w') as fw2, \
        open('../data/name_match.txt','w') as fw3, \
        open('../data/name_fuzzy_match_.txt','w') as fw4:
        for row in f:
            row = row.replace(" ","")
            row = row.replace("\n","")
            ans = name_mismatch(row,fw,fw3)
            if ans == 1:
                name_fuzzy_mismatch(row,fw4)
            if ans == 2: 
                ed_mismatch(row,fw2)


def name_mismatch(row,fw,fw3):
    data = row.split(",")
    query = { "bool" : { "must" : [{ "match" : { "CENSUS_NAMEFRSTB" : data[0] } },
                { "match" : { "CENSUS_NAMELASTB" : data[1] } }] } } 
    res = es.search(index="sample-census", body={ "from": 0, "size": 1, "query":query})
    if res['hits']['total']['value']== 0:
        fw.write(row+"\n")
        return 1
    else:
        fw3.write(row+"\n")
        return 2

def ed_mismatch(row,fw2):
    data = row.split(",")
    query = { "bool" : { "must" : [{ "match" : { "CENSUS_NAMEFRSTB" : data[0] } },
                { "match" : { "CENSUS_NAMELASTB" : data[1] } },
                { "match" : { "CENSUS_ED": data[2]} }] } } 
    res = es.search(index="sample-census", body={ "from": 0, "size": 1, "query":query})
    if res['hits']['total']['value']== 0:
        fw2.write(row+"\n")

def name_fuzzy_mismatch(row,fw4):
    data = row.split(",")
    
    query = { "bool" : { "must" : 
            [{ "fuzzy": { "CENSUS_NAMEFRSTB": { "value": data[0], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
            {"fuzzy": { "CENSUS_NAMELASTB": { "value": data[1], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
            { "geo_distance" : { "distance" : "1km", "LOCATION": { "lat" : round(float(data[-2]),2), "lon" : round(float(data[-1]),2)} } }
            ]}}
    
    # query = { "bool" : { "must" : 
    #         [{ "fuzzy": { "CENSUS_NAMEFRSTB": { "value": "Isaac", "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
    #         {"fuzzy": { "CENSUS_NAMELASTB": { "value": "Aaronsohn", "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
    #         { "geo_distance" : { "distance" : "1km", "LOCATION": { "lat" : 40.72, "lon" : -73.98} } }
    #         ]}}

    res = es.search(index="sample-census", body={ "from": 0, "size": 1, "query":query})
    if res['hits']['total']['value']!= 0:
        fw4.write(row+"\n")


if __name__=='__main__':
    # read_file()
    find_total_umatch()