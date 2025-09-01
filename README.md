# global-demographics-dashboard
ETL pipeline for country demographic data using the REST Countries API.
Automatically extracts, cleans, and aggregates population, area, and density data for all countries, producing both country-level and region-level datasets ready for visualization in dashboards like Power BI or Tableau.

Key Features:

Pulls live data from a public API (Extract)

Cleans and calculates global metrics (Transform)

pushed transformed data to sql database (Load)

Stores raw and processed data in separate folders for reproducibility

Outputs:

countries_raw.json → Raw API data

countries_cleaned.csv → Cleaned, country-level data

region_summary.csv → Aggregated regional metrics
