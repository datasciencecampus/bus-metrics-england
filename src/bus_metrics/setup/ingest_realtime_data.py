"""Class for ingesting realtime BODS data."""

import toml
from dotenv import load_dotenv
import os
import time
from csv import writer
from datetime import datetime
import numpy as np
from bods_client.client import BODSClient
from bods_client.models import BoundingBox, GTFSRTParams
from google.transit import gtfs_realtime_pb2

load_dotenv()


class RealtimeDataIngest:
    """Ingest realtime BODS data.

    Parameters
    ----------
    time_ingest: str
        String representation of date and time

    Attributes
    ----------
    config: dict
        Dictionary of imported toml ingest variables
    api_key: str
        Name of BODS API key variable in .env
    store_data_fp: str
        Root path to storage destination
    message: gtfs_realtime_pb2.FeedMessage
        Protobuf feed message from BODS API

    Methods
    -------
    parse_realtime
        Ingest, parse and store BODS call data to csv

    """

    def __init__(self, time_ingest: str = datetime.now().ctime()):
        self.config: dict = toml.load("src/bus_metrics/setup/ingest.toml")
        self.api_key: str = os.getenv("BODS_API_KEY")
        self.region: str = self.config["region_to_analyse"]
        self.time_ingest: str = time_ingest
        self.store_data_fp: str = f"data/realtime/{self.region}"
        self.message: gtfs_realtime_pb2.FeedMessage = None

    def api_call(self) -> dict:
        """Ingest all bus locations within specified bounding box.

        Returns
        -------
        message.entity: dict
            nested dictionary capturing vehicle, position and timestamp
            attributes for all current bus locations

        """
        bods = BODSClient(api_key=self.api_key)
        bounding_box = BoundingBox(
            **self.config["regions"]["bounds"][self.region]
        )
        params = GTFSRTParams(bounding_box=bounding_box)
        self.message = bods.get_gtfs_rt_data_feed(params=params)

        return None

    def parse_realtime(self, filename: str = None) -> None:
        """Parse API return and write to csv file.

        Parameters
        ----------
        filename: str
            Full filepath to storage location

        """
        packet = self.message.entity
        num_buses = len(packet)

        if filename is None:
            fileTimeStamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            filename = f"{self.store_data_fp}_{fileTimeStamp}.csv"

        with open(filename, "a", newline="") as csv_file:

            # iterate over each bus in 'packet'
            for bus in np.arange(0, num_buses):

                trip_id = packet[bus].vehicle.trip.trip_id

                # only accept buses with valid trip_id
                if trip_id != "":
                    time_ingress = int(time.time())
                    time_transpond = packet[bus].vehicle.timestamp
                    bus_id = packet[bus].vehicle.vehicle.id

                    route_id = packet[bus].vehicle.trip.route_id
                    current_stop = packet[
                        bus
                    ].vehicle.current_stop_sequence  # noqa: E501
                    latitude = packet[bus].vehicle.position.latitude
                    longitude = packet[bus].vehicle.position.longitude
                    bearing = packet[bus].vehicle.position.bearing

                    bus_update = [
                        time_ingress,
                        time_transpond,
                        bus_id,
                        trip_id,
                        route_id,
                        current_stop,
                        latitude,
                        longitude,
                        bearing,
                    ]

                    csv_obj = writer(csv_file)
                    csv_obj.writerow(bus_update)

                else:
                    pass

            csv_file.close()

        return None
