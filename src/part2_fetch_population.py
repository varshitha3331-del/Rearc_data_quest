import os
import json
import logging
import requests
import boto3

logging.basicConfig(level=logging.INFO)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "rearc-data-quest/1.0 (contact: varshitha3331@gmail.com)"
})

DATAUSA_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)

def fetch_population_all_years() -> list[dict]:
    """
    Fetch ALL available population rows from DataUSA API.
    Return rows in simple format:
    [
       {"year": 2010, "population": ...},
       {"year": 2011, "population": ...},
       ...
    ]
    """
    logging.info("Requesting population data from DataUSA API...")
    resp = SESSION.get(DATAUSA_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    # DataUSA format: {"data": [ ... ]}
    records = data.get("data", []) if isinstance(data, dict) else data

    rows: list[dict] = []
    for rec in records:
        try:
            year = int(rec.get("Year"))
            pop_val = rec.get("Population")
            if pop_val is None:
                continue

            rows.append({
                "year": year,
                "population": int(pop_val)
            })
        except:
            # Ignore bad rows
            continue

    # Sort by year
    rows = sorted(rows, key=lambda x: x["year"])

    logging.info("Fetched %d rows total (ALL years)", len(rows))
    return rows


def save_to_s3(rows: list[dict], bucket: str, key: str):
    s3 = boto3.client("s3")
    body = json.dumps(rows).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json"
    )
    logging.info(f"Wrote s3://{bucket}/{key}")


if __name__ == "__main__":
    bucket = os.environ.get("REARC_BUCKET")
    key    = os.environ.get(
        "REARC_POP_KEY",
        "rearc-data-quest/population/us_population_all_years.json"
    )

    assert bucket, "Set REARC_BUCKET first"

    rows = fetch_population_all_years()
    if not rows:
        raise SystemExit("No population rows found — check API")

    save_to_s3(rows, bucket, key)