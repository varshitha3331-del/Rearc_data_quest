import os, time, hashlib, logging, re
from urllib.parse import urljoin
import requests
import boto3

logging.basicConfig(level=logging.INFO)
SESSION = requests.Session()
SESSION.headers.update({

    "User-Agent": "rearc-data-quest/1.0 (contact: varshitha3331@gmail.com)"
})

def list_remote_files(index_url: str):
    r = SESSION.get(index_url, timeout=30)
    r.raise_for_status()
    # If the HTML listing fails, default to pr.data.0.Current
    files = re.findall(r'href=\"([^\"]+pr\.data\.0\.Current[^\"]*)\"', r.text)
    return sorted(set(files)) or ["pr.data.0.Current"]

def download(url: str):
    for i in range(6):  # retry/backoff
        resp = SESSION.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.content
        if resp.status_code in (403, 429, 500, 502, 503, 504):
            time.sleep(2 ** i)
            continue
        resp.raise_for_status()
    resp.raise_for_status()

def md5_bytes(b: bytes):
    return hashlib.md5(b).hexdigest()

def sync(index_url: str, base_url: str, bucket: str, prefix="rearc-data-quest/bls/"):
    s3 = boto3.client("s3")
    files = list_remote_files(index_url)
    for name in files:
        src_url = urljoin(base_url, name)
        logging.info("Fetching %s", src_url)
        blob = download(src_url)
        key = f"{prefix}{name}"
        md5 = md5_bytes(blob)
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
        except Exception:
            head = None
        needs_upload = not head or head.get("Metadata", {}).get("local_md5") != md5
        if needs_upload:
            logging.info("Uploading %s to s3://%s/%s", name, bucket, key)
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=blob,
                ContentType="text/csv",
                Metadata={"local_md5": md5},
            )
        else:
            logging.info("Already up-to-date: %s", name)

if __name__ == "__main__":
    BUCKET = os.environ.get("REARC_BUCKET")
    BASE   = os.environ.get("BLS_BASE", "https://download.bls.gov/pub/time.series/pr/")
    INDEX  = os.environ.get("BLS_INDEX", BASE)
    assert BUCKET, "Set REARC_BUCKET first"
    sync(INDEX, BASE, BUCKET)