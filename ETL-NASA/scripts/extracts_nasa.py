import json
import os
from pathlib import Path
from datetime import datetime
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def extract_apod_data(date=None):
    """
    Extract NASA APOD data for a specific date (YYYY-MM-DD).
    If date is None, returns today's APOD.
    """
    url = "https://api.nasa.gov/planetary/apod"
    api_key = os.getenv("NASA_API_KEY", "DEMO_KEY")  

    params = {
        "api_key": api_key,
    }
    if date:
        params["date"] = date  

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    filename = DATA_DIR / f"nasa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    print(f"Extracted APOD data and saved to: {filename}")
    return data

if __name__== "__main__":
    extract_apod_data()