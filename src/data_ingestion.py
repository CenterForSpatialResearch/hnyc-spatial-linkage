from elasticsearch import Elasticsearch
from argparse import ArgumentParser
import csv,time,logging
import pandas as pd
import numpy as np
from elasticsearch import helpers

es = Elasticsearch('localhost:9200')

logging.basicConfig(filename='../doc/bulk_insert.log',
                            filemode='a',
                            format='%(asctime)s %(name)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.WARNING)
logger = logging.getLogger('InsertTime')

def do_ingest(path, bulk_size, index, id=False):
    df = pd.read_csv(path)
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
        
        if id is not False:
          meta = {
              "_index": index,
              "_id": data[id],
              "_source": data
          }
        else:
          meta = {
              "_index": index,
              "_source": data
          }

        bulk_data.append(meta)
        if itr%bulk_size == 0:
            helpers.bulk(es, bulk_data)
            bulk_data = []
            print("INSERTING NOW", itr)
    helpers.bulk(es, bulk_data)
    return count

if __name__=='__main__':
    if __name__ == '__main__':
      parser = ArgumentParser()
      parser.add_argument("-path", help="file path to insert", default="../data/census1880_sample_LES_v04.csv")
      parser.add_argument("-bulk", help="bulk size insert", default=10000,type=int)
      parser.add_argument("-index", help="index name", default="sample")
      parser.add_argument("-id", help="unique id field name", required=False)

      args = parser.parse_args()
      st = time.time()
      if args.id is False:
        size = do_ingest(args.path,args.bulk,args.index)
      else:
        size = do_ingest(args.path,args.bulk,args.index,args.id)
      end = time.time()
      logger.warning(args.index +" "+ str(size)+" "+ str(end-st))
 
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