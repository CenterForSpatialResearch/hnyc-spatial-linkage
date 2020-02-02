from elasticsearch import Elasticsearch
from argparse import ArgumentParser
import csv,time,logging, json
import pandas as pd
import numpy as np
from elasticsearch import helpers
from metaphone import doublemetaphone

es = Elasticsearch('localhost:9200')

logging.basicConfig(filename='../doc/bulk_insert.log',
                            filemode='a',
                            format='%(asctime)s %(name)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.WARNING)
logger = logging.getLogger('InsertTime')

def ingest(config):
    df = pd.read_csv(config['census_filename'])
    bulk_data = []
    count = 0 
    for itr, row in df.iterrows():
        count+=1
        row = row.replace(np.nan,'',regex=True) #covnert nan to empty string
        data = row.to_dict() #converts the dataframe row to dictonary with their correct data type
        if 'LOCATION' in data:
          data['LOCATION'] = {"lat":data["LAT"],"lon":data["LONG"]}
        
        if 'ADDNUMFROM' in data and type(data['ADDNUMFROM']) is str:
            data['ADDNUMFROM'] = data['ADDNUMFROM'].replace('`','')

        data[config['census_first_name']] = name_clean(data[config['census_first_name']])
        data[config['census_last_name']] = name_clean(data[config['census_last_name']])
        
        if config['metaphone'] is 1:
          data['METAPHONE_NAMEFIRST'] = [i for i in doublemetaphone(data[config['census_first_name']]) if i]
          data['METAPHONE_NAMELAST'] = [i for i in doublemetaphone(data[config['census_last_name']]) if i]
        
        if id is not False:
          meta = {
              "_index": config['es-index'],
              "_id": data[config['es-id']],
              "_source": data
          }
        else:
          meta = {
              "_index": config['es-index'],
              "_source": data
          }

        bulk_data.append(meta)
        if itr%config['ingest_size'] == 0:
            helpers.bulk(es, bulk_data)
            bulk_data = []
            print("INSERTING NOW", itr)
            
    helpers.bulk(es, bulk_data)
    return count

def name_clean(name):
  return max(name.split(' '), key=len)

if __name__=='__main__':
    if __name__ == '__main__':
      parser = ArgumentParser()
      parser.add_argument("-config", help="config file path", default="../data/census1880_sample_LES_v04.csv")

      args = parser.parse_args()

      with open(args.config) as json_data_file:
        config = json.load(json_data_file)


      st = time.time()
      ingest(config)
      end = time.time()
      logger.warning(config["es-index"] +" "+ str(end-st))
 
#Mapping used
'''
PUT census
{
    "mappings" : {
      "properties" : {
        "ADDNUM" : {
          "type" : "long"
        },
        "ADDNUMFROM" : {
          "type" : "long"
        },
        "ADDNUMTO" : {
          "type" : "long"
        },
        "ADDR_TYPE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_ADDRESSB" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_AGEB" : {
          "type" : "long"
        },
        "CENSUS_BUILDING_I" : {
          "type" : "long"
        },
        "CENSUS_CITY" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_ED" : {
          "type" : "long"
        },
        "CENSUS_ENUMDISTB" : {
          "type" : "long"
        },
        "CENSUS_EXTGROUP_I" : {
          "type" : "long"
        },
        "CENSUS_FID" : {
          "type" : "long"
        },
        "CENSUS_MATCH_ADDR" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_MERGEID" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_NAMEFRSTB" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_NAMELASTB" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_NEIGHBOR_1" : {
          "type" : "long"
        },
        "CENSUS_NEIGHBOR_2" : {
          "type" : "long"
        },
        "CENSUS_NPERHHB" : {
          "type" : "long"
        },
        "CENSUS_OCCLABELB" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_PAGENUMB" : {
          "type" : "long"
        },
        "CENSUS_RACEB" : {
          "type" : "long"
        },
        "CENSUS_RACENAMEB" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_REELB" : {
          "type" : "long"
        },
        "CENSUS_RELATEB" : {
          "type" : "long"
        },
        "CENSUS_RELATE_STR" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_SEGGROUP_I" : {
          "type" : "long"
        },
        "CENSUS_SEGMENT_ID" : {
          "type" : "long"
        },
        "CENSUS_SERIAL" : {
          "type" : "long"
        },
        "CENSUS_SERIALB" : {
          "type" : "long"
        },
        "CENSUS_SEXB" : {
          "type" : "long"
        },
        "CENSUS_STATE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_STREET" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_STREETB" : {
          "type" : "long"
        },
        "CENSUS_TYPEB" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_UNITTYPE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "CENSUS_VOLUMEB" : {
          "type" : "long"
        },
        "CITY" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "COUNTY" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "LOCATION":{
             "type": "geo_point"
        },
        "LAT" : {
          "type" : "float"
        },
        "LONG" : {
          "type" : "float"
        },
        "MATCH_ADDR" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "OBJECTID" : {
          "type" : "long"
        },
        "SIDE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STADDR" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STATE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STDIR" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STNAME" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STPREDIR" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STPRETYPE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "STTYPE" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "WARD_NUM" : {
          "type" : "long"
        }
      }
    }
  }
'''