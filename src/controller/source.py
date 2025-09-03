import requests
import pandas as pd
import numpy as np
import json
from pathlib import Path

def extract_countries_data(api_url, raw_dir="data/raw"):
    # Ensure raw directory exists
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    # API URL (only requesting needed fields)
    response = requests.get(api_url)
    response.raise_for_status()

    # Save raw JSON to file
    raw_path = Path(raw_dir) / "countries_raw.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f, indent=2)

    print(f"✅ Raw data saved to {raw_path}")
    return raw_path


def transform_countries_data(raw_path, processed_dir="data/processed"):
    # Ensure processed directory exists
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    # Load raw JSON
    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Convert to DataFrame
    df = pd.DataFrame([
        {
            "name": country.get("name", {}).get("common"),
            "region": country.get("region"),
            "subregion": country.get("subregion"),  # will be None if missing
            "population": country.get("population"),
            "area": country.get("area"),
        }
        for country in data
    ])

    # Clean: remove rows with missing population or area
    df = df.dropna(subset=["population", "area"])

    # Derived metrics
    df["population_density"] = df["population"] / df["area"]
    df["log_population"] = np.log(df["population"])

    # Regional totals for percentages
    df["region_population_total"] = df.groupby("region")["population"].transform("sum")
    df["region_area_total"] = df.groupby("region")["area"].transform("sum")

    df["population_pct_of_region"] = df["population"] / df["region_population_total"]
    df["area_pct_of_region"] = df["area"] / df["region_area_total"]

    # Aggregate by region
    region_summary = df.groupby("region").agg(
        total_population=pd.NamedAgg(column="population", aggfunc="sum"),
        avg_density=pd.NamedAgg(column="population_density", aggfunc="mean"),
        total_area=pd.NamedAgg(column="area", aggfunc="sum"),
        country_count=pd.NamedAgg(column="name", aggfunc="count")
    ).reset_index()

    # Save cleaned data
    cleaned_path = Path(processed_dir) / "countries_cleaned.csv"
    df.to_csv(cleaned_path, index=False)

    # Save region summary
    summary_path = Path(processed_dir) / "region_summary.csv"
    region_summary.to_csv(summary_path, index=False)

    print(f"✅ Cleaned country data saved to {cleaned_path}")
    print(f"✅ Region summary saved to {summary_path}")

    return df, region_summary


