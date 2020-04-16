# Spatial Linkage Web Visualization

This web app visualizes the output of the spatial linkage process for easy identification of errors and subsequent fine-tuning of results.

## How to View
1. Ensure a copy of the data, named `matched_viz.csv` is in a data folder under the `matching_viz` folder. This dataset should be the output of the disambiguation process and have all census and cd record data joined to it. It's available in the Google Drive under outputs too.
2. Open terminal in the `matching_viz` folder. Options to locally host the website (this is a must as otherwise data will not be shown for security reasons):
   1. Using Python: 
      1. `py -m http.server`
      2. Additional guidance available [here](https://developer.mozilla.org/en-US/docs/Learn/Common_questions/set_up_a_local_testing_server#Running_a_simple_local_HTTP_server)
   2. Using Node server: 
      1. To install: `npm install -g http-server`
      2. To run: `http-server &`
   3. Using Live Server extension in VSCode

## Functionality
- Shows all matches spatially distributed on the map
- Mark results of interest