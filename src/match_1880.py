from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json, logging
from metaphone import doublemetaphone
from argparse import ArgumentParser
from elasticsearch import exceptions

es = Elasticsearch('localhost:9200')

logging.basicConfig(filename='../doc/direct_match.log',
                            filemode='a',
                            format='%(created)f %(asctime)s %(name)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.WARNING)
logger = logging.getLogger('directMatch')

def get_matches(args):
    df = pd.read_csv(args.path)
    # bulk_data = []
    # clean_data = {}
    col = "WARD_ED,OBJECTID,MATCH_ADDR,CD_FIRST_NAME,CD_LAST_NAME,CD_MIDDLE_NAME,CD_OCCUPATION,CD_FINAL_HOUSENUM,OBJECTID.x,CENSUS_NAMEFRSCLEAN,CENSUS_NAMELASTB,CENSUS_AGE,CENSUS_OCCLABELB,CENSUS_MATCH_ADDR,CENSUS_SEGMENT_ID"		
    count_match, count_unmatch=0,0
    with open("../data/es-census-1880-1-18-2020","w+") as fw, open("../data/es-census-1880-1-18-2020-unmatch","w+") as fw2:
        fw.write(col+"\n")
        for idx, row in df.iterrows():
            row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
            data = row.to_dict()
            # first_name_metaphone = list(doublemetaphone(data["CD_FIRST_NAME"]))
            # last_name_metaphone = list(doublemetaphone(data["CD_LAST_NAME"]))
            
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
            
            data["CD_FIRST_NAME"] = name_clean(data["CD_FIRST_NAME"])
            data["CD_LAST_NAME"] = name_clean(data["CD_LAST_NAME"])
            
            query = { "bool" : { "must" : [{ "fuzzy": { "CENSUS_NAMEFRSTB": { "value": data["CD_FIRST_NAME"], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
            {"fuzzy": { "CENSUS_NAMELASTB": { "value": data["CD_LAST_NAME"], "fuzziness": 2, "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                    { "match" : { "WARD_NUM": data["WARD_NUM"]} },
                    { "match" : { "CENSUS_ENUMDIST": data["CD_ED"]} },
                    {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
                    {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}
                    ],
                    }}
            
            try:
                res = es.search(index=args.index, body={ "from": 0, "size": 10000, "query":query})
            except exceptions.RequestError:
                print(idx)
                continue
            
            
            if res['hits']['total']['value']!= 0:
                count_match+=1
                output = res['hits']['hits']
                for out in output:
                    out = out["_source"]
                    # print(res)
                    # clean_data[data['OBJECTID']] = res['hits']['hits'][0]['_source']['CENSUS_MERGEID']
                    # try:
                    row = str(data["WARD_NUM"])+"_"+ str(data["CD_ED"])+ ","+str(data["OBJECTID"])+ ","+ data["MATCH_ADDR"]+ "," + data["CD_FIRST_NAME"]+ ","+ data["CD_LAST_NAME"]+ ","+""+ ","+ str(data["CD_OCCUPATION"])+ ","+ str(data["CD_FINAL_HOUSENUM"])
                    row = row + ","+ str(out["OBJECTID.x"])+ "," + out["CENSUS_NAMEFRSTB"]+ ","+ out["CENSUS_NAMELASTB"]+ ","+ str(out["CENSUS_AGE"])+ ","+ out["CENSUS_OCCLABELB"] + ","+ out["CENSUS_MATCH_ADDR"] + ","+ str(out["CENSUS_SEGMENT_ID"])
                    # except KeyError:
                    #     print(out)
                    #     input()
                    fw.write(row+"\n")
            else:
                count_unmatch+=1
                row = str(data["WARD_NUM"])+"_"+ str(data["CD_ED"])+ ","+str(data["OBJECTID"])+ ","+ data["MATCH_ADDR"]+ "," + data["CD_FIRST_NAME"]+ ","+ data["CD_LAST_NAME"]+ ","+""+ ","+ str(data["CD_OCCUPATION"])+ ","+ str(data["CD_FINAL_HOUSENUM"])
                fw2.write(row+"\n")

    logging.warn("Total city directory matched: "+ str(count_match))
    logging.warn("Total city directory unmatched: "+ str(count_unmatch))

def name_clean(name):
  return max(name.split(' '), key=len)

def export_data(data):
    json.dump(data, open('../data/matched_data.json','w'))

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument("-path", help="file path to match with", default="../data/cd1880_sample_LES_v04.csv")
    parser.add_argument("-index", help="index name", default="census-1880")
    args = parser.parse_args()
    get_matches(args)
    # clean_data = get_matches('../data/cd1880_sample_LES_v04.csv')           
    # export_data(clean_data)