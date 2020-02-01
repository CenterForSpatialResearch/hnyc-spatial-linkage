from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json, csv
from metaphone import doublemetaphone
from elasticsearch import exceptions

with open('../doc/config.json') as json_data_file:
    config = json.load(json_data_file)

es = Elasticsearch(host=config['host'], port=config['port'])

def match_addr():
    df = pd.read_csv(config['cd_filename'])
    count, match,unmatch = 0,0,0
    with open(config['match_output_filename'],'w') as fw, open(config['unmatch_output_filename'],'w') as fw2:
        writer = csv.writer(fw)
        columns = config["output_census_cols"] + config["output_city_directory_cols"]
        rows=""
        for cols in columns:
            rows = rows + cols + ","
        
        writer.writerow([rows.rstrip(",")])
        
        # fw.write('CENSUS_IPUMS_UID'+","+'CENSUS_NAMEFRST'+","+'CENSUS_NAMELAST'+","+'CENSUS_WARD_NUM'+","+'OBJECTID'+","+'CD_FIRST_NAME'+","+'CD_LAST_NAME'+","+'CD_ADDRESS'+","+'BLOCK_NUM'+","+'CENSUS_AGE'+","+'CENSUS_DWELLIN_NUM'+","+'CENSUS_DWELLLING_SEQ'+","+'CENSUS_OCCSTR'+","+"CD_OCCUPATION")
        for index, row in df.iterrows():
            row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
            data = row.to_dict()
            
            first_name_metaphone = [i for i in doublemetaphone(data["CD_FIRST_NAME"]) if i]
            last_name_metaphone = [i for i in doublemetaphone(data["CD_LAST_NAME"]) if i]

            if config['edit_distance'] !=0:
                if config['metaphone'] is 1:
                    query = { "bool" : { "must" : [{ "fuzzy": { "CENSUS_NAMEFRST": { "value": data["CD_FIRST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
                            {"fuzzy": { "CENSUS_NAMELAST": { "value": data["CD_LAST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                            { "match" : { "CENSUS_WARD_NUM": data["WARD_NUM"]} },
                            {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
                            {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}]}}

                else:
                    query = { "bool" : { "must" : [{ "fuzzy": { "CENSUS_NAMEFRST": { "value": data["CD_FIRST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } } }, 
                            {"fuzzy": { "CENSUS_NAMELAST": { "value": data["CD_LAST_NAME"], "fuzziness": config["edit_distance"], "max_expansions": 50, "prefix_length": 0, "transpositions": True, "rewrite": "constant_score" } }},
                            { "match" : { "CENSUS_WARD_NUM": data["WARD_NUM"]} }]}}
            

            if config['edit_distance'] is 0:
                if config['metaphone'] is 1:
                   query =  { "bool" : { "must" : [{ "match": { "CENSUS_NAMEFRST": data["CD_FIRST_NAME"] } },
                            { "match": { "CENSUS_NAMELAST": data["CD_LAST_NAME"] } },
                            { "match" : { "CENSUS_WARD_NUM": data["WARD_NUM"]} },
                            {"terms": {"METAPHONE_NAMELAST.keyword": last_name_metaphone}},
                            {"terms": {"METAPHONE_NAMEFIRST.keyword": first_name_metaphone}}]}}
            
                else:
                    query = { "bool" : { "must" : [{ "match": { "CENSUS_NAMEFRST": data["CD_FIRST_NAME"] } },
                            { "match": { "CENSUS_NAMELAST": data["CD_LAST_NAME"] } },
                            { "match" : { "CENSUS_WARD_NUM": data["WARD_NUM"]}}]}}

            
            try:
                res = es.search(index=config["es-index"], body={ "from": 0, "size": 10000, "query":query})
            except exceptions.RequestError:
                print("Exception at row id: ", index)
                continue

            if res['hits']['total']['value']!= 0:
                match+=1
                for i in res['hits']['hits']:
                    i = i['_source']
                    content = ""
                    for j in config["output_census_cols"]:
                        content = content + str(i[j]) + ","

                    # print(data)
                    for j in config["output_city_directory_cols"]:
                        content = content + str(data[j]) + ","
                     
                    # wr = i['CENSUS_IPUMS_UID']+","+i['CENSUS_NAMEFRST']+","+i['CENSUS_NAMELAST']+","+str(i['CENSUS_WARD_NUM'])+","+str(data['OBJECTID'])+","+data['CD_FIRST_NAME']+","+data['CD_LAST_NAME']+","+data['CD_ADDRESS']+","+ str(data['BLOCK_NUM'])+","+str(i['CENSUS_AGE'])+","+ str(i['CENSUS_DWELLIN_NUM'])+","+ str(i['CENSUS_DWELLING_SEQ'])+","+i['CENSUS_OCCSTR']+","+data['CD_OCCUPATION']
                    # fw.write(wr+"\n")
                    print(content)
                    writer.writerow([content.rstrip(",")])
            else:
                fw2.write(str(data['OBJECTID'])+"\n")
                unmatch+=1
        
    print(count,match,unmatch)

if __name__=='__main__':
    match_addr()