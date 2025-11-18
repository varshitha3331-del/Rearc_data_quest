import os
import json
import csv
import io
import logging
import statistics
from collections import defaultdict

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)   

s3 = boto3.client("s3")


def load_population(bucket: str, key: str):
    """Load population JSON (all years) from S3 into a dict: year -> population."""
    logger.info("Loading population from s3://%s/%s", bucket, key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(obj["Body"].read())

    pop_by_year = {}
    for rec in data:
        try:
            year = int(rec.get("year"))
            pop = int(rec.get("population"))
            pop_by_year[year] = pop
        except Exception:
            continue

    logger.info("Loaded %d population rows", len(pop_by_year))
    return pop_by_year


def load_bls(bucket: str, key: str):
    """Load BLS CSV from S3 as list of dict rows."""
    logger.info("Loading BLS data from s3://%s/%s", bucket, key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8", errors="ignore")

    reader = csv.DictReader(io.StringIO(body))
    rows = []
    for row in reader:
        try:
            row["series_id"] = row["series_id"].strip()
            row["period"] = row["period"].strip()
            row["year"] = int(row["year"].strip())
            row["value"] = float(row["value"].strip())
            rows.append(row)
        except Exception:
            continue

    logger.info("Loaded %d BLS rows", len(rows))
    return rows


def compute_population_stats(pop_by_year: dict):
    """Compute mean and std dev for population years 2013–2018."""
    years = list(range(2013, 2019))
    pops = [pop_by_year[y] for y in years if y in pop_by_year]
    if len(pops) < 2:
        logger.warning("Not enough population data for 2013–2018 to compute stats")
        return None, None

    mean_pop = sum(pops) / len(pops)
    std_pop = statistics.pstdev(pops)  # population std dev

    logger.info("Population 2013–2018 mean: %d", int(mean_pop))
    logger.info("Population 2013–2018 std dev: %.2f", std_pop)
    return mean_pop, std_pop


def compute_best_years(bls_rows):
    """
    For every series_id, find the best year:
    year with the largest sum of quarterly values.
    """
    # Sum by (series_id, year) over quarter periods
    sums = defaultdict(float)
    for row in bls_rows:
        period = row["period"]
        if not period.startswith("Q"):
            continue
        key = (row["series_id"], row["year"])
        sums[key] += row["value"]

    # Pick best year per series_id
    best_years = {}
    for (series_id, year), total in sums.items():
        if series_id not in best_years or total > best_years[series_id][1]:
            best_years[series_id] = (year, total)

    logger.info("Computed best years for %d series_ids", len(best_years))

    # Log a few sample entries
    count = 0
    for sid, (year, total) in best_years.items():
        logger.info("Best year sample - series_id=%s, year=%d, year_sum=%.4f", sid, year, total)
        count += 1
        if count >= 5:
            break

    return best_years


def compute_prs30006032_q01(bls_rows, pop_by_year):
    """
    Build rows for series_id=PRS30006032 and period=Q01
    joined with population (if available).
    """
    results = []
    for row in bls_rows:
        if row["series_id"] == "PRS30006032" and row["period"] == "Q01":
            year = row["year"]
            value = row["value"]
            pop = pop_by_year.get(year)
            results.append({
                "series_id": row["series_id"],
                "year": year,
                "period": row["period"],
                "value": value,
                "Population": pop,
            })

    logger.info("Found %d rows for PRS30006032 Q01", len(results))
    # Log a few sample rows
    for r in results[:5]:
        logger.info(
            "PRS30006032 row - year=%d, period=%s, value=%.4f, population=%s",
            r["year"], r["period"], r["value"], r["Population"]
        )
    return results


def lambda_handler(event, context):
    """
    Lambda B: triggered by SQS messages.
    For each batch, it:
      - loads BLS and population data from S3
      - computes:
          * population stats 2013–2018
          * best year per series
          * PRS30006032 Q01 + population
      - logs the results to CloudWatch
    """
    bucket = os.environ["REARC_BUCKET"]
    bls_key = os.environ.get("BLS_KEY", "rearc-data-quest/bls/pr.data.0.Current")
    pop_key = os.environ.get("POP_KEY", "rearc-data-quest/population/us_population_all_years.json")

    logger.info("Lambda analytics triggered by SQS. Records in event: %d", len(event.get("Records", [])))

    # Load data
    pop_by_year = load_population(bucket, pop_key)
    bls_rows = load_bls(bucket, bls_key)

    # Part 3.1: population stats
    compute_population_stats(pop_by_year)

    # Part 3.2: best year per series_id
    compute_best_years(bls_rows)

    # Part 3.3: PRS30006032 Q01 + population
    compute_prs30006032_q01(bls_rows, pop_by_year)

    logger.info("Analytics Lambda completed successfully")
    return {"ok": True}