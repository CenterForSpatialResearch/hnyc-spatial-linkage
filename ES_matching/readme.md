## Elastic Search
1. Install elasticsearch from: https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html
2. Once elasticsearch is installed, make sure the elastic cluster is running. To run in Linux/Unix (Mac): `./bin/elasticsearch` in the elasticsearch extracted folder.
3. Clone repository
4. Go to repository root folder and run `pip install -r requirements.txt` to install dependencies.

To run scripts:
1. Data ingestion for 1880 census data: `python data_ingestion.py -config ../doc/config_1880.json`
2. Data ingestion for 1850 census data: `python data_ingestion.py -config ../doc/config_1850.json`
3. Match data for 1880: `python match_1880.py`
4. Match data for 1850: `python match_1850.py
