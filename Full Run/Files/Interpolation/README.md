# Interpolation

This folder contains all the files with functions and classes necessary for the interpolation process as currently
developed. It's structured in order to make adding to and modifying the process simple.

1. `disambiguation_analysis.py`: This contains the functions needed to analyze the disambiguation output and
fill in all records within a dwelling with a match, resolving any conflicts in matches at the dwelling level.
    - how to implementing these functions is demonstrated in the notebook Disambiguation_Analysis_v01.ipynb in the
    Process_Documentation folder
    
2. `__init__.py`: This contains class definitions and attributes for building models for prediction
    -`CensusData`: This class is designed to take in a dataframe of all census data post disambiguation, with dwelling 
    match conflicts resolved and records within a single dwelling filled in appropriately. It's method functions apply
    processing to the census data so that it's set up for use in interpolation, the new dataframe is stored in the attribute
    df. 
    - For a new way to process/format this data simply add a new method function, and save the result in the df attribute
    - Column names can be specified in a creation of an instance, the defaults represent the current column names in 1850. 
    Once column names are standardized please change the defaults accordingly
    
    -`Interpolator`: This is a parent class, from which specific types of interpolators inherit. It's designed so that 
    an interpolation instance operates within a specified ward. It's set up to take in a CensusData variable, a ward number, 
    a model for prediction, and a set of features names. Its method functions are related to training and testing the
    model
    - Methods pertaining to the model itself can be added here
    - This is structured so that new ways of interpolating (ei predicting street name) can inherited from this class
    - This is not meant to be instantiated itself, but rather provides a template, and method functions that apply to
    all child classes
    
    -`BlockInterpolator`: This is a child of the interpolator class, it's y which is set to the block_col, since that's 
    what it predicts.
    
    -`CentroidInterpolator`: This is a child of the interpolator class, in addition to the interpolator contructor input,
    it takes a block_centroid dictionary, which is a nested dictionary containing ward numbers corresponding to a dictionary
    of bock numbers within that ward and their centroids. It also takes a clustering algorithm. It's y attribute is set 
    to "cluster", which is the name of the column that cluster values are stored in when apply_clustering is run. Its 
    method functions are related to running, tuning, and visualizing the clustering algorithm and its results.

3. `interpolation.py`: Contains functions relating to dwelling fill in and analysis, as well as for interpolation
4. `sequences.py`: Contains functions for sequence creation
5. `dataprocessing.py`: Contains functions related to dataframe manipulation, ei joining dwellings to all records, etc.
6. `archive.py`: Contains functions that are no longer needed, but kept for reference as they are used in older
development notebooks
7. `interpolation_fillin.py`: Contains functions for the reworked interpolation phase (including merging the enumeration detail and the census data). 
    
