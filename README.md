# Rearc Data Engineering Quest

This project contains my solution for the Rearc Data Engineering Quest.

### Contents
- Step 1 and Step 2 Python source code: `src/`
- Step 3 Jupyter Notebook: `rearc_data_quest_part3.ipynb`
- Step 4 Terraform infrastructure: `terraform/`
- Lambda source code: `lambda_ingest/` and `lambda_analytics/`

### S3 Data Link
S3 location of final data output:
(put your S3 link here)

### How to run (summary)
1. Install Python packages: `pip install -r requirements.txt`
2. Run BLS sync script: `python src/part1_sync_bls.py`
3. Run population fetch script: `python src/part2_fetch_population.py`
4. To run infrastructure:
    ```
    cd terraform
    terraform init
    terraform apply
    ```
