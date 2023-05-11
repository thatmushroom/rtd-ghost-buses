""" Functions to parse and extract data from the .pb files """
import os
import pathlib

import logging
import pandas as pd

from google.transit import gtfs_realtime_pb2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_PRIVATE = os.getenv("BUCKET_PRIVATE", "rtd-ghost-buses-private")
BUCKET_PUBLIC = os.getenv("BUCKET_PUBLIC", "rtd-ghost-buses-public")


def run():
    utils_path = os.path.join(pathlib.Path(os.getcwd()).parent, "utils")
    alerts_filename = os.path.join(utils_path, "Alerts.pb")
    tripupdate_filename = os.path.join(utils_path, "TripUpdate.pb")

    vehicleposition_filename = os.path.join(utils_path, "VehiclePosition.pb")
    file_types = ["Alerts", "TripUpdate", "VehiclePosition"]
    for file_type in file_types:
        match file_type:
            case "Alerts":
                with open(alerts_filename, "rb") as f:
                    pb_message = f.read()
            case "TripUpdate":
                with open(tripupdate_filename, "rb") as f:
                    pb_message = f.read()
            case "VehiclePosition":
                with open(vehicleposition_filename, "rb") as f:
                    pb_message = f.read()
        decoded_message = gtfs_decode(pb_message, file_type)

        # print(f'file_type: {file_type}\n')
        # print(f'decoded message: {decoded_message}\n')
        for entity in decoded_message.entity:
            if entity.HasField("trip_update"):
                print(entity.trip_update)
            if entity.HasField("vehicle"):
                print(entity.vehicle)


def gtfs_decode(pb_message, message_type):
    """RTD uses three protobuf message types. This returns an object of the correct type with the file read in"""

    match message_type:
        case "Alerts":
            try:
                alert = gtfs_realtime_pb2.FeedMessage()
                alert.ParseFromString(pb_message)
                return alert
            except:
                return None
        case "TripUpdate":
            try:
                trip_update = gtfs_realtime_pb2.FeedMessage()
                trip_update.ParseFromString(pb_message)
                return trip_update
            except:
                return None
        case "VehiclePosition":
            try:
                vehicle_position = gtfs_realtime_pb2.FeedMessage()
                vehicle_position.ParseFromString(pb_message)
                return vehicle_position
            except:
                return None
        case _:
            return "Command not recognized"


if __name__ == "__main__":
    run()
