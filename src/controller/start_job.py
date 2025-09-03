import requests
import pandas as pd
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
from datetime import datetime
from controller.source import *
from sqlalchemy import create_engine
from controller.target import * 

def start_job():

    #load environment variables from .env file

    load_dotenv()  

    url = os.getenv("RESTCOUNTRIES_URL")

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST")
    posgres_port = os.getenv("POSTGRES_PORT")

    status_success = 200
    status_failure = 500

    # Step 0: Setup DB connection

    db_connection = create_engine(f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{posgres_port}/{postgres_db}")

    # Step 1: Extract Data

    try:
        raw_path = extract_countries_data(url)
        logging.info(f"Data extraction complete. Raw data saved at: {raw_path}")
    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        return f"Job failed at extraction step with error: {e}", status_failure
    
    # Step 2: Transform Data

    try:
        cleaned_path, summary_path = transform_countries_data(raw_path)
        logging.info(f"Data transformation complete. Cleaned data saved at: {cleaned_path}, Summary saved at: {summary_path}")
    except Exception as e:
        logging.error(f"Data transformation failed: {e}")
        return f"Job failed at transformation step with error: {e}", status_failure
    
    # Step 3: Load Data to PostgreSQL

    try:
       message, status = load_countries_data_to_postgres(cleaned_path, summary_path, db_connection)
       logging.info(message)
       return message, status
    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        return f"Job failed at loading step with error: {e}", status_failure