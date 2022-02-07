This is a full run of the record linkage pipeline from ES to Interpolation on 1850 data. 
Before running the notebook, upload all input files (census, cd, and enumeration details) as well as supportive python files (in the "Files" section). 
Also, install all the required packages. 

## Current Results on 1850 Data (Fall 2021) 
###  Initial Matching (ES) 

| | Manhattan | Brooklyn |
|---------- | --------- | -------- |
| Matched | 60748 | 7887| 
| Unmatched | 33364 | 5021|
| Exception | 0 | 34| 

*Exceptions with Brooklyn data is due to the missing ward number. 

### Initial Matching (continued) 
#### Manhattan 
Number of cd records matched is 23756. 
Proportion of cd records in ES included in final match is 0.973. 
#### Brooklyn
Number of cd records matched is 4420. 
Proportion of cd records in ES included in final match is 0.980. 

### After Interpolation
#### Manhattan 
Proportion of records without any address info (block or street) is 0.4795. 
Proportion of records without any address info (block or street) after interpolation and modeling is 0.3747. 

## Future Steps 
1. Apply the whole process until the end of interpolation on Brooklyn 1850 data
2. Use the prediction of block number and street id to draw detailed forecast on address information
3. Standardize file and variable names 
4. Create main.py to run through the whole process
5. Check and clean current codes
6. Consider incorporating features generated in experiments into the whole process
7. Standardize address information in cd records
