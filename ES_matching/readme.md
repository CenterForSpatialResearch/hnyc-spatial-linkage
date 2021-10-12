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

For non-Linux users:
1. Install elasticsearch from: https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html
2. Open cmd, cd to folder elasticsearch-7.7.0/bin, type `elasticsearch` to run cluster
   - Ensure JAVA_HOME is set to JDK location, check by typing `echo %JAVA_HOME%` in cmd, cmd should return path to JDK
     - if not set properly, in system Environment Variables, ensure variable `JAVA_HOME` exists and is set to the correct path (e.g. `C:\Program Files\Java\jdk1.8.0_231`)
   - If there's an error regarding log files not being writable, run cmd as admin (right click on cmd icon > 'Run as Adminstrator'), and type `attrib -r -s <FULL PATH TO ELASTICSEARCH LOG FOLDER>`
     - e.g. `attrib -r -s "C:\Program Files\elasticsearch-7.7.0\logs"`
     - this will give write permissions to log files
3. Run `pip install -r requirements.txt` to install dependencies
4. Run scripts as above!


# Matching Output Statistics

Pruthvi's trials

|        Type       	| Manhattan 	| Brooklyn 	|
|-------------------	|-----------	|----------	|
| Total Records     	|           	|          	|
| Matched Records   	| 60581     	| 7821     	|
| Unmatched Records 	| 33426     	| 5062     	|
