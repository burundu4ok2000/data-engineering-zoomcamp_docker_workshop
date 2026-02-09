#!/usr/bin/env python3
"""
Load Yellow Taxi Parquet files (Jan-Jun 2024) to GCS.
"""

import os
import subprocess
from google.cloud import storage

# Configuration
BUCKET_NAME = "de-zoomcamp-2026-485615-yellow-taxi-2024"
PROJECT_ID = "de-zoomcamp-2026-485615"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

MONTHS = ["01", "02", "03", "04", "05", "06"]
YEAR = "2024"


def download_file(url: str, filename: str) -> bool:
    """Download file using wget."""
    print(f"Downloading {filename}...")
    result = subprocess.run(
        ["wget", "-q", "-O", filename, url],
        capture_output=True
    )
    return result.returncode == 0


def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str):
    """Upload file to GCS bucket."""
    print(f"Uploading {source_file} to gs://{bucket_name}/{destination_blob}...")
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"  ✓ Uploaded!")


def create_bucket_if_not_exists(bucket_name: str, location: str = "US"):
    """Create GCS bucket if it doesn't exist."""
    storage_client = storage.Client(project=PROJECT_ID)
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except Exception:
        print(f"Creating bucket {bucket_name}...")
        bucket = storage_client.create_bucket(bucket_name, location=location)
        print(f"  ✓ Created bucket {bucket_name}")


def main():
    # Create bucket
    create_bucket_if_not_exists(BUCKET_NAME)
    
    # Download and upload each month
    for month in MONTHS:
        filename = f"yellow_tripdata_{YEAR}-{month}.parquet"
        url = f"{BASE_URL}/{filename}"
        local_path = f"/tmp/{filename}"
        
        # Download
        if not os.path.exists(local_path):
            if not download_file(url, local_path):
                print(f"  ✗ Failed to download {filename}")
                continue
        else:
            print(f"File {local_path} already exists, skipping download.")
        
        # Upload to GCS
        upload_to_gcs(BUCKET_NAME, local_path, filename)
        
        # Clean up local file
        # os.remove(local_path)
    
    print("\n✓ All files uploaded!")
    print(f"\nVerify with: gsutil ls gs://{BUCKET_NAME}/")


if __name__ == "__main__":
    main()
