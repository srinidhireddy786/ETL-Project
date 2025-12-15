import pandas as pd
import json
import os
import glob

def transform_nasa_apod():

    os.makedirs("../data/staged", exist_ok=True)
    files = glob.glob("../data/raw/nasa_*.json")
    if not files:
        raise FileNotFoundError("No APOD raw JSON files found.")
    latest_file = sorted(files)[-1]

    with open(latest_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame([{
        "date": data.get("date"),
        "title": data.get("title"),
        "explanation": data.get("explanation"),
        "media_type": data.get("media_type"),
        "image_url": data.get("url") or data.get("hdurl"),
        "inserted_at": pd.Timestamp.now()
    }])

    output_path = "../data/staged/nasa_apod_cleaned.csv"
    df.to_csv(output_path, index=False)

    print(f"Transformed APOD record saved to: {output_path}")
    return df


if __name__ == "__main__":
    transform_nasa_apod()