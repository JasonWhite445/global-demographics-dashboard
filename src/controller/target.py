import pandas as pd
from pathlib import Path
from datetime import datetime

def load_countries_data_to_postgres(cleaned_path, region_path, db_connection):
        
        """
        Load cleaned country data and regional summary into PostgreSQL database.
        
        args:
        - cleaned_path: Path to the cleaned countries CSV file.
        - region_path: Path to the regional summary CSV file.
        - db_connection: SQLAlchemy database connection engine.
        
        Returns:
        - message: Success message.
        
        """

        # Add ingestion timestamp
        now = datetime.now()
        cleaned_path['ingestion_timestamp'] = now
        region_path['ingestion_timestamp'] = now

        # Load to PostgreSQL
        cleaned_path.to_sql('countries', db_connection, if_exists='append', index=False)
        region_path.to_sql('region_summary', db_connection, if_exists='append', index=False)

        print("✅ Data loaded to PostgreSQL successfully")
        return "Job completed successfully.", 200