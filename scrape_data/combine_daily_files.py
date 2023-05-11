""" Assemble pb to parquet dataframes, one per day """
import sys
from google.protobuf.json_format import MessageToJson
import json
from google.transit import gtfs_realtime_pb2
from io import BytesIO
import boto3
import pandas as pd
import pendulum


class GTFSdata:
    def __init__(self):
        self.s3 = boto3.resource("s3")
        self.bucket = self.s3.Bucket("rtd-ghost-buses-private")

    trip_descriptor_message = {
        "trip_id": str,
        "route_id": str,
        "direction_id": int,
        "start_time": str,
        "start_date": str,
        "schedule_relationship": str,
    }

    # https://developers.google.com/transit/gtfs-realtime/guides/feed-entities

    def gtfs_decode(self, pb_message):
        """RTD uses three protobuf message types. This returns an object of the correct type with the file read in"""
        match self.data_type:
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

    @staticmethod
    def entity_to_dataframe(message, filename):
        """Convert one decoded message to a pandas dataframe"""
        if message is None:
            return None
        if len(message.entity) == 0:
            return None
        dataframe_rows = []
        for entity in message.entity:
            row = pd.json_normalize(json.loads(MessageToJson(entity)))
            dataframe_rows.append(row)

        message_df = pd.concat(dataframe_rows)
        message_df["header.timestamp"] = pd.Timestamp.utcfromtimestamp(
            message.header.timestamp
        )
        message_df["filename"] = filename
        return message_df

    def dataset_to_dataframe(self):
        """Convert the entire decoded dataset"""
        entire_dataframe_rows = []
        for k, v in self.data_dict_decoded.items():
            entire_dataframe_rows.append(self.entity_to_dataframe(v, k))
        self.dataset_df = pd.concat(entire_dataframe_rows)
        return None

    def decode_s3_data(self):
        """Apply gtfs_decode tp a dict"""
        self.data_dict_decoded = {
            k: self.gtfs_decode(v) for k, v in self.data_dict.items()
        }
        # self.data_dict_decoded = dict(self.data_dict, map(gtfs_decode, self.data_dict.values()))

        return None

    def fetch_s3_data(self, day):
        """Given a date range, fetch the un-decoded files"""

        date_str = day.to_date_string()
        print(f"Processing {date_str} at {pendulum.now().to_datetime_string()}")
        # Prefix
        objects = self.bucket.objects.filter(
            Prefix=f"bus_data_{self.data_type}/{date_str}"
        )

        print(f"------- loading data at {pendulum.now().to_datetime_string()}")

        # load data
        data_dict = {}

        for obj in objects:
            print(f"loading {obj}")
            obj_name = obj.key
            obj_body = obj.get()["Body"].read()
            data_dict[obj_name] = obj_body

        self.data_dict = data_dict

        return len(data_dict)

    def upload_s3_parquet(self, day):
        # Upload the s3 file to s3.
        # self.bucket.
        date_str = day.to_date_string()
        out_buffer = BytesIO()
        self.dataset_df.to_parquet(out_buffer)
        out_buffer.seek(0)
        self.bucket.upload_fileobj(
            out_buffer, f"processed/{self.data_type}/{date_str}.parquet"
        )
        return None

    # Master function - fetches data - converts - saves as parquet - upload to s3, all looped one day at a time
    def run(self, date_range):
        """Master run"""
        for day in date_range:
            _ = self.fetch_s3_data(day)
            self.decode_s3_data()
            self.dataset_to_dataframe()
            self.upload_s3_parquet(day)


class VehiclePosition(GTFSdata):
    def __init__(self):
        self.data_type = "VehiclePosition"
        self.field_types = [
            field.name for field in gtfs_realtime_pb2._VEHICLEPOSITION.fields
        ]
        # self.sparse_cols_list = ['vehicle.trip.tripId',	'vehicle.trip.scheduleRelationship',	'vehicle.trip.routeId',	'vehicle.trip.directionId',	"vehicle.currentStatus",	'vehicle.stopId'] # Parquet does not like sparse dtypes
        super().__init__()


class TripUpdate(GTFSdata):
    def __init__(self):
        self.data_type = "TripUpdate"
        self.field_types = [
            field.name for field in gtfs_realtime_pb2._TRIPUPDATE.fields
        ]
        # self.sparse_cols_list = ['tripUpdate.stopTimeUpdate',	'tripUpdate.vehicle.id',	'tripUpdate.vehicle.label'] # Parquet does not like sparse dtypes
        super().__init__()


class Alerts(GTFSdata):
    def __init__(self):
        self.data_type = "Alerts"
        self.field_types = [field.name for field in gtfs_realtime_pb2._ALERT.fields]
        super().__init__()


if __name__ == "__main__":
    # python combine_daily_files.py '2023-01-01' '2023-01-01' &
    start_date = sys.argv[1]  #'2023-01-04' # YYYY-MM-DD
    end_date = sys.argv[2]  # '2023-01-04' # YYYY-MM-DD

    # start_date = '2023-01-04' # YYYY-MM-DD
    # end_date = '2023-01-04' # YYYY-MM-DD

    date_range = [
        d
        for d in pendulum.period(
            pendulum.from_format(start_date, "YYYY-MM-DD"),
            pendulum.from_format(end_date, "YYYY-MM-DD"),
        ).range("days")
    ]
    print(date_range)
    vp = VehiclePosition()
    vp.run(date_range)
    tu = TripUpdate()
    tu.run(date_range)
    alerts = Alerts()
    alerts.run(date_range)
