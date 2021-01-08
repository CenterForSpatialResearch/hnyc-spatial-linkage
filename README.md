# HNYC Spatial Research on Spatial Linkages

## Fall 2020
1. Improvised elastic search match process to include more records for both 1880 and 1850 (see `ES_matching/src` folder - Elastic_Search_1850_mn v02.ipynb)
2. Disambiguation process on new matches (see `disambiguation_1850` folder) 
3. Resolved dwelling conflicts on the new 1850 disambiguated output (that includes new matches) (`interpolation_notebooks/Concepts_and_Development/Dwelling_Addresses_Fill_In_and_Conflict_Resolution_Development_v2.ipynb`)
4. Updated 'dwelling adress fill in' process to identify unique dwelling ids and then choose dwelling address through bipartite matching both for summer and fall 1850 disambiguated run (`interpolation_notebooks/Concepts_and_Development/Unique_Dwelling_Addresses_Fill_In_and_Conflict_Resolution_Development.ipynb`)

potential for improvement
1. Explore rules for elastic search match
2. Identify other weight combinations for confidence score tuning
3. Explore methods for creating unique dwelling ids for interpolation process
4. Make comparisons in the output of Summer and Fall 2020 (with extra matches) disambiguated and address fill in processes

## Summer 2020
1. Developed confidence score tuning process (see `disambiguation_1880` folder)
2. Developed interpolation process (see `interpolation` folder for code and `interpolation_notebooks` folder for jupyter notebooks that document process)
3. For overview of process and next steps see google drive `HNYC_Project/Projects/spatial_linkage/Spatial Linkage & Interpolation: Summer 2020.ppt` and `HNYC_Project/Projects/spatial_linkage/Spatial Linkage and Interpolation Workflow.doc`

## Spring 2020 Actions
Accomplished:  
1. Updated confidence score to include census conflicts (see `disambiguation_2.ipynb`)
2. Merged lat lng data to matched data -> produced `matched.csv` 
3. Experimented with methods to add spatial weights, including graph-based and cluster-based approaches on a subset of the data (see `spatial-disambiguation.ipynb`)
4. Outlined overall workflow for disambiguation using bipartite graph matching algorithm (`linkage-disambiguation.ipynb`).
5. Ran algorithms on full dataset & obtained initial performance metrics
   1. Functionalized process using disambiguation module
6. Tuned algorithms and compared metrics + recommendations
7. Updated ES matching process to reflect metaphone matching
_See Google Drive Documentation > disambiguation > Spatial Linkage: Spring 2020 for slides_

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
  - `explore_output.ipynb` quick validation of the ES output (targeted at understanding whether metaphone matching was implemented in ES process)
- `/disambiguation_1850` contains all work related to disambiguation of 1880 data
  - `disambiguation_1850_v1.ipynb` runs disambiguation process on 1850 ES output
- `/disambiguation_1880` contains all work related to disambiguation of 1880 data
  - `Confidence_Score_Tuning.ipynb`: Documents confidence score tuning process
  - `Confidence_Score_Tuning_v02.ipynb`: Confidence score tuning results used for 1850 v02 disambiguation run (10/2020), uses old version of 1880 data because of data issues with new 1880
  - `Confidence_Score_Tuning_new_1880_data_draft.ipynb`: Attempt to tune confidence score with new 1880 data, revealed that there was an issue with the data
  - `/_archived`: archived scripts
    - `/confidence_score`
      - `preprocess.ipynb`: preprocessing of data including generation of metaphones
      - `get_confidence_score.ipynb`: outlines process for generating confidence score, including calculation of jaro-wrinkler
      - `disambiguation_analysis.ipynb`: EDA on confidence scores -- generates `match_results_confidence_score.csv` and `fall_2019_disambiguation_report.md`
    - `/linkage_eda`
      - `spatial-disambiguation.ipynb`: documentation of different spatial weight algorithms
      - `linkage-disambiguation.ipynb`: outline record linkage approach (conceptually valid but code is outdated)
      - `linkage_full_run_v1.ipynb`: applies basic algorithm to the whole dataset + initial performance analysis
      - `linkage_eda.ipynb`: applies various iterations of algorithm to the whole dataset + conclusions 
      - `linkage_eda_v2.ipynb`: applies updated geocodes on best 2 algorithms + improved benchmarking
  - `run_link_records.ipynb`: implemented record matching using pyspark
  - `confidence_score_latlng.ipynb`: adding of census conflicts to confidence score, merging of lat lng data (contains most updated confidence score formula)
  - `linkage_full_run_SPRING_LATEST.ipynb`: informed by linkage EDA (see archive), generates latest disambiguated output from ES matching (with metaphone issue fixed)
- `/interpolation_notebooks`
    - `Process_Documentation`
       - `Disambiguation_Analysis_v01.ipynb`: Resolves dwelling conflicts, calculates statistics,explores distance based sequences, and interpolation between known dwellings   
       - `Interpolation_v01.ipynb`: Runs through current version of predicting unknown records
       - `Disambiguation_Analysis_v02.ipynb`: Same information as v01, for new data (10/2020)   
       - `Interpolation_v02.ipynb`: Same information as v01, for new data (10/2020)
     - `Concepts_and_Development`:
       - `Block and Centroid Prediction with Analysis.ipynb`: Walks through approaches to predicting block numbers directly, and then clusters (tests different clustering algorithms)
       - `Block Centroids and What They Represent, 1850.ipynb`: Creates block centroids and illustrates them with visualizations
       - `Dwelling Addresses Fill In and Conflict Resolution Development.ipynb`: Development of conflict resolution within dwelling process
       - `Developing Distance Based Sequences.ipynb`: Process of developing distance based sequences
       - `Model Comparison.ipynb`: Tests a few different model options (no in depth tuning)
       - `Sequences Exploration.ipynb`: Tests different iterations of sequence identification
       -  `Model Exploration.ipynb`: Brief experimentation with using neural networks, incomplete because of preprocessing necessary
     - `Archived` 
       - `1880_1850_for_Interpolation.ipynb`: Explores 1880 and 1850 census datasets
       - `Feature_Exploration.ipynb`: Explores some of the columns in 1880 and 1850 datasets in order to determine what they represents and if they can be used for modelling
       - `Interpolation Pilots.ipynb`: Working notebook for starting explorations of options for interpolation (often moved into a separate notebook when they seem worth looking at in more depth)
       - `Linear_Model.ipynb`: Creates and tests linear models for house number interpolation
       - `Modeling Comparison.ipynb`: Tests different modeling approaches for house numbers (currently linear model and gradient boosting) -- includes haversine sequences and block numbers as features
       - `Block_Numbers Early Exploration.ipynb`: Explore block numbers distributions/data analysis and try using them as feature to predict house number
       - `Street_Dictionaries.ipynb`: Tried out looking at street dictionaries for dwellings in between
       - `Block Number Prediction.ipynb`: Initial experiment with predicting block numbers
       - `Interpolation between known address development.ipynb`: Process of looking at values between known dwellings
- `/interpolation` See read me within this folder for details
- `/disambiguation` is a python module containing wrapper functions needed in the disambiguation process
  - `init.py` contains a Disambiguator object, when instantiated can be used to run entire disambiguation process, calling functions from below (see `linkage_eda.ipynb` for example on usage)
  - `preprocess.py` contains functions needed before applying disambiguation algorithms, including confidence score generation
  - `disambiguation.py` contains functions needed for disambiguation
  - `analysis.py` wrapper functions to produce performance metrics
  - `confidence_score_tuning.py` contains functions needed for the confidence tuning process
  - `benchmarking.py` contains Benchmark objects, to run benchmarking process in confidence tuning for 1880
- `/matching_viz` visualization web app to understand disambiguation output, see readme in folder for guidance on how to run

## Data
Data is available in the HNYC Spatial Linkage Google Drive `HNYC_Project/Projects/spatial_linkage/Data`

### 1850 `/1850`
- 1850 disambiguated output: `1850_disambiguated.csv`
- 1850 disambiguated output 10/2020 (current): `1850_mn_match_v02.csv`
- 1850 ES matches: `es-1850-22-9-2020.csv`

### 1880
- Matches with confidence score (raw input for 1880 disambiguation processes): `matches.csv`
  - this is based off Fall 2019's Spark matching output
- Latest ES matches: `es-1880-21-5-2020.csv`
- Latest Disambiguated Output: `disambiguated-21-5-2020.csv`
