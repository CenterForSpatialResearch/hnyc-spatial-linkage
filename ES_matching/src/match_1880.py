from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json, logging, csv
from metaphone import doublemetaphone
from argparse import ArgumentParser
from elasticsearch import exceptions

with open('../doc/config_1880.json') as json_data_file:
    config = json.load(json_data_file)


es = Elasticsearch('localhost:9200')
logging.basicConfig(filename='../doc/direct_match.log',
                            filemode='a',
                            format='%(created)f %(asctime)s %(name)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.WARNING)
logger = logging.getLogger('directMatch')

def get_matches():
    df = pd.read_csv(config['cd_filename'])
    count_match, count_unmatch=0,0

    with open(config['match_output_filename'],'w') as fw, open(config['unmatch_output_filename'],'w') as fw2:
        writer = csv.writer(fw, delimiter="\t")
        columns = config["output_census_cols"] + config["output_city_directory_cols"]
        rows=""

        for cols in columns:
            rows = rows + cols + "\t"
        
        writer.writerow(rows.rstrip("\t").split("\t"))
        
        for idx, row in df.iterrows():
            row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
            data = row.to_dict()
        
            data["CD_FIRST_NAME"] = name_clean(data["CD_FIRST_NAME"])
            data["CD_LAST_NAME"] = name_clean(data["CD_LAST_NAME"])
            
            first_name_metaphone = [i for i in doublemetaphone(data["CD_FIRST_NAME"]) if i]
            last_name_metaphone = [i for i in doublemetaphone(data["CD_LAST_NAME"]) if i]

            if config['edit_distance'] !=0:
                if config['metaphone'] is 1:
                    query = { "bool" : { "must" : [{ "fuzzy": { "CENSUS_FIRST_NAME": { "value": data["CD_FIRST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } },
                            {"fuzzy": { "CENSUS_LAST_NAME": { "value": data["CD_LAST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                                    { "match" : { "CENSUS_WARD_NUM": data["CD_WARD_NUM"]} },
                                    { "match" : { "CENSUS_ED": data["CD_ED"]} },
                                    {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
                                    {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}]
                            }}

                else:
                    query = { "bool" : { "must" : [{ "fuzzy": { "CENSUS_FIRST_NAME": { "value": data["CD_FIRST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } },
                            {"fuzzy": { "CENSUS_LAST_NAME": { "value": data["CD_LAST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                                    { "match" : { "CENSUS_WARD_NUM": data["CD_WARD_NUM"]} },
                                    { "match" : { "CENSUS_ED": data["CD_ED"]}}
                                    ],
                            }}
            

            if config['edit_distance'] is 0:
                if config['metaphone'] is 1:
                    query = { "bool" : { "must" : [ { "match" : { "CENSUS_WARD_NUM": data["CD_WARD_NUM"]} },
                            #{ "match" : { "CENSUS_ED": data["CD_ED"]} }, #add this back in once ed column data is fixed
                            {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
                            {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}
                            ]}}
            
                else:
                    query = { "bool" : { "must" : [{ "match": { "CENSUS_FIRST_NAME": data["CD_FIRST_NAME"] } },
                            { "match": { "CENSUS_LAST_NAME": data["CD_LAST_NAME"] } },
                            { "match" : { "CENSUS_WARD_NUM": data["CD_WARD_NUM"]} },
                            { "match" : { "CENSUS_ED": data["CD_ED"]} }
                            ]}}

            
            try:
                res = es.search(index=config["es-index"], body={ "from": 0, "size": 10000, "query":query})
            except exceptions.RequestError as e:
                print(idx)
                continue
        
            if res['hits']['total']['value']!= 0:
                count_match+=1
                for i in res['hits']['hits']:
                    i = i['_source']
                    content = ""
                    for j in config["output_census_cols"]:
                        content = content + str(i[j]) + "\t"

                    for j in config["output_city_directory_cols"]:
                        content = content + str(data[j]) + "\t"

                    writer.writerow(content.rstrip("\t").split("\t"))

                fw2.write(str(data['CD_RECORD_ID'])+"\n")
                count_unmatch+=1
            

    logging.warning("Total city directory matched: "+ str(count_match))
    logging.warning("Total city directory unmatched: "+ str(count_unmatch))

def name_clean(name):
  return max(name.split(' '), key=len)

def export_data(data):
    json.dump(data, open('../../data/matched_data.json','w'))

if __name__=='__main__':
    get_matches()
