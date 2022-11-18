import os

import boto3
import json
import logging
import math
import pandas as pd
import pendulum
import requests
import urllib


from google.transit import gtfs_realtime_pb2


# import utils.gtfs_realtime_pb2 as gtfs_rt
# import gtfs_realtime_pb2 as gtfs_rt
# use for dev, but don't deploy to Lambda:
# from dotenv import load_dotenv
# load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_PRIVATE = os.getenv("BUCKET_PRIVATE", "chn-ghost-buses-private")
BUCKET_PUBLIC = os.getenv("BUCKET_PUBLIC", "chn-ghost-buses-public")

feed = gtfs_realtime_pb2.FeedMessage()

response = requests.get('https://www.rtd-denver.com/files/gtfs-rt/TripUpdate.pb')
feed.ParseFromString(response.content)
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print(entity.trip_update)
    
# TODO: 
# Scrape that hits RTD's endpoints and decodes the .pb file 
# 
# https://www.rtd-denver.com/files/gtfs-rt/Alerts.pb
def rtd_scrape(url):
    """ Scrape  """
    print(f"scraping {url}") # TODO logger()
    response = requests.get(url) # Get the data
    
def gtfs_decode(pb_message, message_type):
    """ RTD uses three protobuf message types. This returns an object of the correct type """
    match message_type:
        case 'Alerts':
            alert = gtfs_realtime_pb2.FeedMessage()
            return alert.ParseFromString(pb_message)
        case 'TripUpdate':
            trip_update = gtfs_realtime_pb2.FeedMessage()
            return trip_update.ParseFromString(pb_message)
        case 'VehiclePosition':
            vehicle_position = gtfs_realtime_pb2.FeedMessage()
            return vehicle_position.ParseFromString(pb_message)
        case _:
            return 'Command not recognized'

for entity in alert.entity:
    if entity.HasField('alert'):
        print(entity.alert)
# for entity in trip_update.entity:
#     if entity.HasField('trip_update'):
#         print(entity.trip_update)
# for entity in vehicle_position.entity:
#     if entity.HasField('trip_update'):
#         print(entity.trip_update)

def local_gtfs_wrapper():
    """ Test the .pb files """
    # alert, trip_update, vehicle_position = local_gtfs_wrapper()
    with open("Alerts.pb","rb") as f:
        alert_file = f.read()
        # alert = gtfs_decode(f.read(), "Alerts")
    print(alert)
    alert = gtfs_realtime_pb2.FeedMessage()
    alert.ParseFromString(alert_file)
    with open("TripUpdate.pb","rb") as f:
        trip_update = gtfs_decode(f.read(), "TripUpdate")
    print(trip_update)
    with open("VehiclePosition.pb","rb") as f:
        vehicle_position = gtfs_decode(f.read(), "VehiclePosition")
    print(vehicle_position)
    return alert, trip_update, vehicle_position

    
         

def rtd_scrape_wrapper():
    base_url = "https://www.rtd-denver.com/files/gtfs-rt/"
    message_types = ["Alerts", "TripUpdate", "VehiclePosition"]
    
    

def scrape(routes_df, url):
    bus_routes = routes_df[routes_df.route_type == 3]
    response_json = json.loads("{}")
    for chunk in range(math.ceil(len(bus_routes) / 10)):
        chunk_routes = routes_df.iloc[
            [chunk * 10 + i for i in range(10)],
        ]
        route_query_string = chunk_routes.route_short_name.str.cat(sep=",")
        logger.info(f"Requesting routes: {route_query_string}")
        try:
            chunk_response = json.loads(
                requests.get(url + f"&rt={route_query_string}" + "&format=json").text
            )
            response_json[f"chunk_{chunk}"] = chunk_response
        except requests.RequestException as e:
            logger.error("Error calling API")
            logger.error(e)
    logger.info("Data fetched")
    return response_json


def lambda_handler(event, context):
    API_KEY = os.environ.get("CHN_GHOST_BUS_CTA_BUS_TRACKER_API_KEY") # TODO: RTD API key
    s3 = boto3.client("s3")
    # tuple of the form: (version label as used in URL, version label to append to top-level directory name)
    for api_version in [("v2", ""), ("v3", "_v3")]:
        logger.info(f"Hitting API version {api_version[0]}")
        api_url = (
            f"http://www.ctabustracker.com/bustime/api"
            f"/{api_version[0]}/getvehicles?key={API_KEY}"
        )
        routes_df = pd.read_csv(
            # TODO: we can move the to-scrape route list to the public bucket later
            s3.get_object(Bucket=BUCKET_PRIVATE, Key="current_routes.txt")["Body"]
        )
        logger.info("Loaded routes df")
        data = json.dumps(scrape(routes_df, api_url))
        logger.info("Saving data")
        t = pendulum.now("America/Denver")
        for bucket in [BUCKET_PUBLIC, BUCKET_PRIVATE]:
            key = f"bus_data{api_version[1]}/{t.to_date_string()}/{t.to_time_string()}.json"
            logger.info(f"Writing to {key}")
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
            )
