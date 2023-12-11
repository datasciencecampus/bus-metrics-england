import logging
from datetime import datetime
import toml
import os
from os import listdir
import glob
import pandas as pd
import shutil
from src.utils.preprocessing import (
    deduplicate,
    zip_files,
    unzip_GTFS,
    convert_string_time_to_unix,
    build_stops,
)  # noqa: E501
from src.utils.resourcing import import_file_from_naptan


class GTFS_Builder:

    """
    Takes realtime and timetable data for a specified day, deduplicating
    and replacing timetabled times with actual realtime times. An updated
    GTFS folder is returned.

    Args:
        config (dict): data_ingest content of config file.
    """

    def __init__(
        self,
        tt_region: str = "Yorkshire",
        rt_region: str = "YorkshireandTheHumber",
        region: str = "YorkshireandTheHumber",
        date: str = "20231103",
        dir: str = "data/daily",
        output: str = "data/output",
        timetable_exceptions: bool = True,
        zip_gtfs: bool = True,
        unzip_timetable: bool = False,
        route_stop_threshold: int = 5,
        route_types: list = [3],
        logger=None,
    ):
        self.tt_region = tt_region
        self.rt_region = rt_region
        self.region = region
        self.date = date
        self.dir = dir
        self.output = output
        self.timetable_exceptions = timetable_exceptions
        self.zip_gtfs = zip_gtfs
        self.unzip_timetable = unzip_timetable
        self.route_stop_threshold = route_stop_threshold
        self.route_types = route_types
        self.weekday = datetime.strptime(date, "%Y%m%d").strftime("%A").lower()

        # Initialise logger
        if logger is None:
            self.logger = logging.getLogger(__name__)

        else:
            self.logger = logger

    def load_raw_realtime_data(self, region=None) -> pd.DataFrame:
        """
        Collects all realtime data for individual day

        Returns:
            df (pandas df): unprocessed realtime data
        """

        if region is None:
            region = self.rt_region

        dir = f"data/daily/realtime/{region}/{self.date}/"
        # collate all realtime ingests to single dataframe
        tables = os.path.join(dir, "*.csv")  # noqa: E501
        tables = glob.glob(tables)
        dtypes_dict = {
            "time_ingest": int,
            "time_transpond": int,
            "bus_id": str,
            "trip_id": str,
            "route_id": str,
            "current_stop": int,
            "latitude": float,
            "longitude": float,
            "bearing": float,
            "journey_date": int,
        }

        df_list = (pd.read_csv(table, dtype=dtypes_dict) for table in tables)
        df = pd.concat(df_list, ignore_index=True)

        df = df.iloc[:, 1:]
        df = df.sort_values("time_ingest")
        df.reset_index(drop=True, inplace=True)
        return df

    def split_realtime_data(self, df) -> (pd.DataFrame, pd.DataFrame):
        """
        Collects and concatenates all realtime data for specified day

        Returns:
            labelled_real (pandas df): rows with trip_id & route_id
            unlabelled_real (pandas df): row with trip_id & route_id MISSING
        """

        labelled_real = unlabelled_real = df[
            (~df["trip_id"].isna()) & (~df["route_id"].isna())
        ]

        unlabelled_real = df[(df["trip_id"].isna()) & (df["route_id"].isna())]

        return labelled_real, unlabelled_real

    def load_raw_timetable_data(self, region=None) -> pd.DataFrame:
        """
        Collects and concatenates all timetable data for specified
        day, returning all individual service stops (every 'bus
        at stop' activity) for the day.

        Returns:
            service_stops (pandas_df): all service stops
        """

        if region is None:
            region = self.tt_region

        from_dir = f"{self.dir}/timetable/{region}/{self.date}"

        if self.unzip_timetable:
            # Unzip timetable.zip
            unzip_GTFS(
                txt_path=from_dir,
                zip_path=from_dir,
                file_name_pattern="timetable.zip",
                logger=logger,
            )

        # id services affected by exceptions today only
        calendar_dates = pd.read_csv(
            f"{from_dir}/calendar_dates.txt",
            dtype={"service_id": int, "date": int, "exception_type": int},
        )
        calendar_dates = calendar_dates[
            calendar_dates["date"] == int(self.date)
        ]  # must be integer

        # N.B. exception_type -> 1: added, 2: dropped
        exception_adds = list(
            calendar_dates["service_id"][calendar_dates["exception_type"] == 1]
        )
        exception_drops = list(
            calendar_dates["service_id"][calendar_dates["exception_type"] == 2]
        )

        trips = pd.read_csv(
            f"{from_dir}/trips.txt",
            engine="python",
            dtype={
                "route_id": str,
                "service_id": int,
                "trip_id": str,
                "trip_headsign": str,
                "block_id": str,
                "shape_id": str,
                "wheelchair_accessible": int,
                "vehicle_journey_code": str,
            },
        )

        stop_times = pd.read_csv(
            f"{from_dir}/stop_times.txt",
            dtype={
                "trip_id": str,
                "arrival_time": str,
                "departure_time": str,
                "stop_id": str,
                "stop_sequence": int,
                "stop_headsign": str,
                "pickup_type": int,
                "drop_off_type": int,
                "shape_dist_traveled": float,
                "timepoint": int,
                "stop_direction_name": str,
            },
        )

        routes = pd.read_csv(
            f"{from_dir}/routes.txt",
            dtype={
                "route_id": str,
                "agency_id": str,
                "route_short_name": str,
                "route_long_name": float,
                "route_type": int,
            },
        )

        # filter only bus type (not coach, ferry, metro)
        routes = routes[routes["route_type"].isin(self.route_types)]

        stops = build_stops()

        service_stops = pd.merge(
            pd.merge(
                pd.merge(trips, stop_times, on="trip_id"),
                stops,
                on="stop_id",
                how="left",  # noqa: E501
            ),
            routes,
            on="route_id",
            how="left",
        )

        calendar = pd.read_csv(
            f"{from_dir}/calendar.txt",
            dtype={
                "service_id": int,
                "monday": int,
                "tuesday": int,
                "wednesday": int,
                "thursday": int,
                "friday": int,
                "saturday": int,
                "sunday": int,
                "start_date": int,
                "end_date": int,
            },
        )

        if self.timetable_exceptions:
            # drop rows for dropped services by exception
            calendar = calendar[~calendar["service_id"].isin(exception_drops)]

            # filter only services for today's weekday
            calendar = calendar[calendar[self.weekday] == 1]

            # active services plus those added by exception
            active_services = list(calendar["service_id"].unique())
            active_services = list(set(active_services) | set(exception_adds))

            # filter service stops to active services only
            service_stops = service_stops[
                service_stops["service_id"].isin(active_services)
            ]  # noqa: E501

        # add initial column with today's date
        service_stops.insert(loc=0, column="timetable_date", value=self.date)
        service_stops = service_stops.dropna(subset={"route_type"})

        # drop any service stops running after 23:59:59
        service_stops = service_stops[
            service_stops["arrival_time"].str[:2].astype("int") < 24
        ]

        service_stops = convert_string_time_to_unix(
            self.date, service_stops, "arrival_time", convert_type="column"
        )  # noqa: E501

        return service_stops

    def prepare_gtfs(self, labelled_real, timetable) -> pd.DataFrame:
        """
        Extracts all timetable rows aligned to trip_id active in realtime data,
        replacing timetabled times with realtime times.

        Args:
            labelled_real (pandas df): rows with trip_id & route_id
            timetable (pandas df): exploded timetable data

        Returns:
            gtfs_temp (pandas df): exploded timetable data with realtime times
        """
        trip_ids = labelled_real["trip_id"].dropna().unique()  # 1856 trip_ids

        # filter timetable to include only the trip_ids reflected
        # in the RT data
        tt = timetable[timetable["trip_id"].isin(trip_ids)]

        # merge timetable time and stop sequence data side-by-side
        tt_rt_merged = pd.merge(
            tt[["timetable_date", "trip_id", "stop_sequence"]],
            labelled_real[["time_transpond", "trip_id", "current_stop"]],
            left_on=["trip_id", "stop_sequence"],
            right_on=["trip_id", "current_stop"],
        )

        # convert unix time to string
        # NB this assumes conversion to GMT at the moment!
        # tt_rt_merged["dt_time_transpond"] = pd.to_datetime(
        #     tt_rt_merged["time_transpond"], unit="s"
        # ).dt.strftime("%H:%M:%S")

        tt_rt_merged["dt_time_transpond"] = pd.to_datetime(
            tt_rt_merged["time_transpond"], unit="s"
        )
        tt_rt_merged["dt_time_transpond"] = (
            tt_rt_merged["dt_time_transpond"]
            .apply(lambda x: x.tz_localize("UTC").tz_convert("Europe/London"))
            .dt.strftime("%H:%M:%S")
        )

        # recreate tt with ACTUAL times injected
        gtfs_temp = pd.merge(
            tt,
            tt_rt_merged[["trip_id", "stop_sequence", "dt_time_transpond"]],
            on=["trip_id", "stop_sequence"],
        )

        # copy transpond times to arrival time and maintain axis location
        gtfs_temp["arrival_time"] = gtfs_temp["dt_time_transpond"]

        # aribtrary copying transpond times to departure time
        # (does this matter?)
        gtfs_temp["departure_time"] = gtfs_temp["dt_time_transpond"]

        gtfs_temp = gtfs_temp.drop(columns=["dt_time_transpond"])

        gtfs_temp = gtfs_temp.convert_dtypes({"route_type": "int"})

        gtfs_temp = gtfs_temp.sort_values(["trip_id", "stop_sequence"])

        return gtfs_temp

    def write_gtfs(self, gtfs_temp):
        """
        Write/export updated realtime/timetable data to individual GTFS files.

        Args:
            gtfs_temp (pandas_df): exploded timetable data with realtime times
        """

        from_dir = f"{self.dir}/timetable/{self.tt_region}/{self.date}"
        to_dir = f"{self.output}"

        if not os.path.exists(to_dir):
            os.mkdir(to_dir)

        # reverse-engineered trips.txt
        gtfs_temp[
            [
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
                "block_id",
                "shape_id",
                "wheelchair_accessible",
                # "trip_direction_name",
                "vehicle_journey_code",
            ]
        ].drop_duplicates().to_csv(
            f"{to_dir}/trips.txt", index=False
        )  # noqa: E501

        # reverse-engineered stop_times.txt
        gtfs_temp[
            [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
                "stop_headsign",
                "pickup_type",
                "drop_off_type",
                "shape_dist_traveled",
                "timepoint",
                # "stop_direction_name",
            ]
        ].to_csv(f"{to_dir}/stop_times.txt", index=False)

        # reverse-engineered stop_times.txt
        gtfs_temp[
            [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_type",
            ]
        ].drop_duplicates().to_csv(
            f"{to_dir}/routes.txt", index=False
        )  # noqa: E501

        cal_dates = pd.DataFrame(
            columns=["service_id", "date", "exception_type"]
        )  # noqa: E501
        cal_dates.to_csv(f"{to_dir}/calendar_dates.txt", index=False)  # noqa: E501

        # arbitrarily adding the first date we see
        #  - programmatic approach required for multiple days
        date = gtfs_temp.loc[0, "timetable_date"]
        calendar = pd.DataFrame(
            gtfs_temp["service_id"].unique(), columns=["service_id"]
        )

        # set calendar columns up
        calendar[
            [
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ]
        ] = (0, 0, 0, 0, 0, 0, 0, date, date)

        # Set 1 to weekayday column
        calendar[self.weekday] = 1

        calendar.to_csv(f"{to_dir}/calendar.txt", index=False)

        # copy other admin files across - no changes required
        shutil.copy(
            f"{from_dir}/agency.txt",
            f"{to_dir}/agency.txt",  # noqa: E501
        )
        shutil.copy(
            f"{from_dir}/feed_info.txt",
            f"{to_dir}/feed_info.txt",  # noqa: E501
        )
        shutil.copy(
            f"{from_dir}/shapes.txt",
            f"{to_dir}/shapes.txt",  # noqa: E501
        )
        shutil.copy(f"{from_dir}/stops.txt", f"{to_dir}/stops.txt")  # noqa: E501

        if self.zip_gtfs:
            zip_name = f"{self.tt_region}_{self.date}_realtimegtfs.zip"
            zip_files(to_dir, zip_name)

            # remove individual GTFS .txt components
            for fileName in listdir(to_dir):
                if fileName.endswith(".txt"):
                    os.remove(f"{to_dir}/{fileName}")
            self.logger.info("Output folder cleaned")

        return None


if __name__ == "__main__":
    # define session_id that will be used for log file and feedback
    session_name = f"bus_gtfs_build_{format(datetime.now(), '%Y_%m_%d_%H:%M')}"
    logger = logging.getLogger(__name__)
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        filename=f"log/{session_name}.log",
        filemode="a",
    )

    # load toml config
    config = toml.load("config.toml")
    builder = GTFS_Builder(**config["generic"], **config["data_ingest"])

    if not os.path.exists("data/daily/gb_stops.csv"):
        logger.info("Importing stops from NaPTAN site")
        import_file_from_naptan()

    logger.info("Loading all realtime data")
    real = builder.load_raw_realtime_data()

    labelled_real, unlabelled_real = builder.split_realtime_data(real)
    logger.info(f"Raw labelled realtime data: {len(labelled_real)} rows")
    logger.info(f"Raw UNLABELLED realtime data: {len(unlabelled_real)} rows")

    labelled_real = deduplicate(labelled_real)
    unlabelled_real = deduplicate(unlabelled_real)
    logger.info(f"Dedup labelled realtime data: {len(labelled_real)} rows")
    logger.info(f"Dedup UNLABELLED realtime data: {len(unlabelled_real)} rows")

    logger.info("Building stops data from NaPTAN")
    stops = build_stops(logger=logger)

    logger.info("Loading all timetable data")
    timetable = builder.load_raw_timetable_data()

    logger.info(
        "Extract timetable data aligned to realtime activity \
            - inject real times"
    )
    gtfs = builder.prepare_gtfs(labelled_real, timetable)

    logger.info("Write updated GTFS files")
    builder.write_gtfs(gtfs_temp=gtfs)

    if config["data_ingest"]["zip_gtfs"]:
        logger.info("Zipping GTFS files")

    logger.info("GTFS Builder complete....")
