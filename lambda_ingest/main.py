import os
import logging

# We assume these two files are in the same folder as main.py inside the zip
# and have the functions defined.
from part1_sync_bls import sync as sync_bls
from part2_fetch_population import fetch_population_all_years, save_to_s3


logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    """
    Lambda A: runs Part 1 (BLS sync) + Part 2 (Population API).

    Environment variables expected:
      - REARC_BUCKET : target S3 bucket name
      - BLS_BASE     : (optional) base URL for BLS
      - BLS_INDEX    : (optional) index/listing URL for BLS
      - REARC_POP_KEY: (optional) S3 key for population JSON
    """
    bucket = os.environ["REARC_BUCKET"]

    base = os.environ.get(
        "BLS_BASE",
        "https://download.bls.gov/pub/time.series/pr/"
    )
    index = os.environ.get("BLS_INDEX", base)

    pop_key = os.environ.get(
        "REARC_POP_KEY",
        "rearc-data-quest/population/us_population_all_years.json"
    )

    logger.info("Starting ingest Lambda")
    logger.info("Bucket: %s", bucket)

    # ---- Part 1: sync BLS data to S3 ----
    logger.info("Syncing BLS time-series data from %s", base)
    sync_bls(index, base, bucket)
    logger.info("Finished BLS sync")

    # ---- Part 2: fetch population and save to S3 ----
    logger.info("Fetching population data from DataUSA")
    rows = fetch_population_all_years()
    if not rows:
        logger.warning("No population rows fetched from API")
    else:
        save_to_s3(rows, bucket, pop_key)
        logger.info("Saved %d population rows to %s", len(rows), pop_key)

    logger.info("Ingest Lambda completed successfully")
    return {"ok": True}