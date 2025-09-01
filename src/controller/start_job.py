import requests
import pandas as pd
import json
from pathlib import Path
import logging
from datetime import datetime
from controller.source import extract_countries_data, transform_countries_data

def start_job():

    url = "https://restcountries.com/v3.1/all?fields=name,region,subregion,population,area"
    status_success = 200
    status_failure = 500


    #Step 1: Extract Data
    try:
        raw_path = extract_countries_data(url)
        logging.info(f"Data extraction complete. Raw data saved at: {raw_path}")
    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        return f"Job failed at extraction step with error: {e}", status_failure
    
    #Step 2: Transform Data
    try:
        cleaned_path, summary_path = transform_countries_data(raw_path)
        logging.info(f"Data transformation complete. Cleaned data saved at: {cleaned_path}, Summary saved at: {summary_path}")
        return "Job completed successfully.", status_success
    except Exception as e:
        logging.error(f"Data transformation failed: {e}")
        return f"Job failed at transformation step with error: {e}", status_failure



    

