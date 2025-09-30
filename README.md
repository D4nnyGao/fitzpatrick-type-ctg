
# Fitzpatrick Skin Type Clinical Trials Map

This project visualizes the geographic distribution of clinical trials across the United States that mention the Fitzpatrick skin type scale in their eligibility criteria.

You can view and interact with the live map here:
**https://d4nnygao.github.io/fitzpatrick-type-ctg/**

## Interactive Map

The `index.html` file in the root directory is an interactive map built with Folium. It displays all identified clinical trial locations in the US. Users can filter the displayed locations based on several criteria:
* Fitzpatrick Skin Type (I-VI)
* Minimum patient enrollment
* Study status (e.g., Recruiting, Completed)
* Participant race demographics

## Data Retrieval and Filtering

The data for this project is sourced directly from the **ClinicalTrials.gov API**. A Python script (`usa_map/map.py`) automates the entire process.

1.  **API Query**: The script sends a request to the API, specifically searching for studies located in the **United States** where the term "**fitzpatrick**" appears in the eligibility criteria.
2.  **Initial Parsing**: The raw JSON response, containing hundreds of studies, is saved. The script then parses each study's eligibility text to identify the specific Fitzpatrick skin types being included or excluded.
3.  **Data Cleaning & Standardization**: It extracts and standardizes the skin type scores into a consistent format (e.g., converting ranges like "II-V" into individual flags for types II, III, IV, and V).
4.  **Geocoding & Final Dataset**: The script extracts the geographic coordinates for each study facility, along with other key details like enrollment numbers, study status, and participant demographics. This cleaned and structured data is then saved as a `.csv` file, which is used to generate the interactive map.