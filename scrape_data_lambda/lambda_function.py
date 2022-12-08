import os
import boto3
import logging
import pendulum
import requests


logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_PRIVATE = os.getenv("BUCKET_PRIVATE", "rtd-ghost-buses-private")
BUCKET_PUBLIC = os.getenv("BUCKET_PUBLIC", "rtd-ghost-buses-public")
    
def lambda_handler(event, context):
    s3 = boto3.client("s3")
    message_types = ["Alerts", "TripUpdate", "VehiclePosition"]
    base_url = "https://www.rtd-denver.com/files/gtfs-rt/"
    for message_type in message_types:
        api_url = f"{base_url}{message_type}.pb"
        logger.info(f"Scraping data for {message_type}")
        response = requests.get(api_url)
        t = pendulum.now("America/Denver")
        if response.status_code == 200:
            data = response.content 
            logger.info("Saving data")
            for bucket in [BUCKET_PUBLIC, BUCKET_PRIVATE]:
                key = f"bus_data_{message_type}/{t.isoformat()}.pb"
                logger.info(f"Writing to {key}")
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data,
                )
        else:
            logger.error(f"Did not retrieve data at {t}")