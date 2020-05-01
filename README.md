# HNYC Spatial Research on Spatial Linkages

## Spring 2020 Actions
Accomplished:  
1. Updated confidence score to include census conflicts (see `disambiguation_2.ipynb`)
2. Merged lat lng data to matched data -> produced `matched.csv` 
3. Experimented with methods to add spatial weights, including graph-based and cluster-based approaches on a subset of the data (see `spatial-disambiguation.ipynb`)
4. Outlined overall workflow for disambiguation using bipartite graph matching algorithm (`linkage-disambiguation.ipynb`).
5. Ran algorithms on full dataset & obtained initial performance metrics
6. Tuned algorithms and compared metrics + recommendations

## Summary of Processes Taken in Fall 2019
### 1880 Process
Two sources:  
- **City Directory**: name (first, last, initial), address, ID, occupation, ED, ward, block number (constructed)  
- **Census**: address (hidden during match process), ID (different from CD), occupation, ED, ward, age, gender, dwelling, household characteristics

Steps:  
0. Preprocessing: changing names to their phonetic index  
1. Initial entity link (to generate possible matches): criteria = same ED + JW dist < 2 on indexed name  
2. Disambiguation (to choose between non-unique matches):  
  a. Generate a confidence score based on occupation (having occupation), age > 12 (**not implemented**), JW dist of name, relative probability (number of non-unique matches)  

### 1850
Similar, but no ED and address data, only ward data in the census.

## Sitemap
- `/ES_matching` contains all work related to elastic search
  - `readme.md` gives guidelines on how to implement the process
  - `fall_2019_analysis.md` describes the work done up to fall 2019
  - `/doc`: relevant documentation of the process
  - `/src`: source code for running elastic search on our data
- `/disambiguation_1880` contains all work related to disambiguation of 1880 data
  - `preprocess.ipynb`: preprocessing of data including generation of metaphones
  - `run_link_records.ipynb`: implemented fuzzy matching using pyspark
  - `get_confidence_score.ipynb`: outlines process for generating confidence score, including calculation of jaro-wrinkler
  - `disambiguation_analysis.ipynb`: EDA on confidence scores -- generates `match_results_confidence_score.csv` and `fall_2019_disambiguation_report.md`
  - `disambiguation_2.ipynb`: adding of census conflicts to confidence score, merging of lat lng data
  - `spatial-disambiguation.ipynb`: documentation of different spatial weight algorithms
  - `linkage-disambiguation.ipynb`: outline record linkage approach
  - `linkage_full_run_v1.ipynb`: applies basic algorithm to the whole dataset + initial performance analysis
  - `linkage_eda.ipynb`: applies various iterations of algorithm to the whole dataset + conclusions 
  - `linkage_eda_v2.ipynb`: applies updated geocodes on best 2 algorithms
- `/disambiguation` is a python module containing wrapper functions needed in the disambiguation process
  - `init.py` contains a Disambiguator object, when instantiated can be used to run entire disambiguation process, calling functions from below (see `linkage_eda.ipynb` for example on usage)
  - `preprocess.py` contains functions needed before applying disambiguation algorithms
  - `linkage.py` contains functions needed for disambiguation
  - `analysis.py` wrapper functions to produce performance metrics