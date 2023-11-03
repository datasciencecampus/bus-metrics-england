import logging
from datetime import datetime
import toml
import os
import glob
import pandas as pd
import shutil

# from src.utils.stop_sequence import nearest_neighbor
from src.utils.preprocessing import deduplicate, zip_files


class GTFS_Builder:

    """
    Takes realtime and timetable data for a specified day, deduplicating
    and replacing timetabled times with actual realtime times. An updated
    GTFS folder is returned.

    Args:
        config (dict): data_ingest content of config file.
    """

    def __init__(self, config):
        self.today = config["today"]
        self.weekday = config["weekday"]
        self.region = config["region"]
        self.dir = f"{config['dir']}/{self.region}/{self.today}"
        self.output = config["output"]
        self.timetable_exceptions = config["timetable_exceptions"]
        self.zip_gtfs = config["zip_gtfs"]

    def load_raw_realtime_data(self) -> pd.DataFrame:
        """
        Collects all realtime data for individual day

        Returns:
            df (pandas df): unprocessed realtime data
        """

        to_dir = f"{self.dir}/realtime/"
        # collate all realtime ingests to single dataframe
        tables = os.path.join(to_dir, "realtime*.csv")  # noqa: E501
        tables = glob.glob(tables)
        df = pd.concat(map(pd.read_csv, tables), ignore_index=True)
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

        unlabelled_real.to_csv(
            f"{self.dir}/realtime/{self.region}_{self.today}_unlabelled.csv"
        )
        labelled_real.to_csv(
            f"{self.dir}/realtime/{self.region}_{self.today}_labelled.csv"
        )

        return labelled_real, unlabelled_real

    def load_raw_timetable_data(self) -> pd.DataFrame:
        """
        Collects and concatenates all timetable data for specified
        day, returning all individual service stops (every 'bus
        at stop' activity) for the day.

        Returns:
            service_stops (pandas_df): all service stops
        """

        to_dir = f"{self.dir}/timetable"

        # id services affected by exceptions today only
        calendar_dates = pd.read_csv(
            f"{to_dir}/calendar_dates.txt",
            dtype={"service_id": int, "date": int, "exception_type": int},
        )
        calendar_dates = calendar_dates[
            calendar_dates["date"] == int(self.today)
        ]  # must be integer

        # N.B. exception_type -> 1: added, 2: dropped
        exception_adds = list(
            calendar_dates["service_id"][calendar_dates["exception_type"] == 1]
        )
        exception_drops = list(
            calendar_dates["service_id"][calendar_dates["exception_type"] == 2]
        )

        trips = pd.read_csv(
            f"{to_dir}/trips.txt",
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
        stops = pd.read_csv(
            f"{to_dir}/stops.txt",
            dtype={
                "stop_id": str,
                "stop_code": str,
                "stop_name": str,
                "stop_lat": float,
                "stop_lon": float,
                "wheelchair_boarding": int,
                "location_type": float,
                "parent_station": str,
                "platform_code": str,
            },
        )
        stop_times = pd.read_csv(
            f"{to_dir}/stop_times.txt",
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
            f"{to_dir}/routes.txt",
            dtype={
                "route_id": str,
                "agency_id": str,
                "route_short_name": str,
                "route_long_name": float,
                "route_type": int,
            },
        )

        # filter only bus type (not coach, ferry, metro)
        routes = routes[routes["route_type"] == 3]

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
            f"{to_dir}/calendar.txt",
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
        service_stops.insert(loc=0, column="timetable_date", value=self.today)

        return service_stops

    # def assign_stop_sequence_to_reclaimed(self, reclaimed, timetable):

    #     assigned_df = pd.DataFrame(columns=list(reclaimed.columns))

    #     for trip in reclaimed["trip_id"].unique():

    #         trip_route = timetable[timetable["trip_id"] == trip]
    #         trip_route_gdf = gpd.GeoDataFrame(
    #             trip_route.reset_index(drop=True),
    #             geometry=gpd.points_from_xy(
    #                 trip_route["stop_lat"], trip_route["stop_lon"]
    #             ),
    #             crs=4326,
    #         )
    #         trip_route_gdf.to_crs(27700)

    #         if len(trip_route) > 0:

    #             trip_reclaim = reclaimed[reclaimed["trip_id"] == trip]
    #             trip_reclaim_gdf = gpd.GeoDataFrame(
    #                 trip_reclaim.reset_index(drop=True),
    #                 geometry=gpd.points_from_xy(
    #                     trip_reclaim["latitude"], trip_reclaim["longitude"]
    #                 ),
    #                 crs=4326,
    #             )
    #             trip_reclaim_gdf.to_crs(27700)

    #             reclaim_aligned_gdf = nearest_neighbor(
    #                 trip_reclaim_gdf, trip_route_gdf, return_dist=True
    #             )
    #             reclaim_nearest_gdf = reclaim_aligned_gdf.loc[
    #                 reclaim_aligned_gdf.groupby(
    #                     "current_stop"
    #                 ).distance.idxmin()  # noqa: E501
    #             ]

    #             reclaim_nearest_gdf = (
    #                 reclaim_nearest_gdf[
    #                     reclaim_nearest_gdf["distance"]
    #                     < self.nearest_threshold  # noqa: E501
    #                 ]
    #                 .sort_values("time_transpond")
    #                 .reset_index(drop=True)
    #             )

    #             assigned_df = pd.concat([assigned_df, reclaim_nearest_gdf])

    #         else:
    #             pass

    #     return assigned_df.reset_index(drop=True)

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
            tt[["timetable_date", "arrival_time", "trip_id", "stop_sequence"]],
            labelled_real[["time_transpond", "trip_id", "current_stop"]],
            left_on=["trip_id", "stop_sequence"],
            right_on=["trip_id", "current_stop"],
        )

        # convert times
        tt_rt_merged["unix_arrival_time"] = (
            tt_rt_merged["timetable_date"].astype("str")
            + " "
            + tt_rt_merged["arrival_time"]
        )
        tt_rt_merged["unix_arrival_time"] = pd.to_datetime(
            tt_rt_merged["unix_arrival_time"], format="%Y%m%d %H:%M:%S"
        )
        tt_rt_merged["unix_arrival_time"] = (
            tt_rt_merged["unix_arrival_time"] - pd.Timestamp("1970-01-01")
        ) // pd.Timedelta("1s")
        tt_rt_merged["unix_arrival_time"] = (
            tt_rt_merged["unix_arrival_time"] - 3600
        )  # adjust to GMT
        tt_rt_merged["dt_arrival_time"] = pd.to_datetime(
            tt_rt_merged["time_transpond"] + 3600, unit="s"
        ).dt.strftime("%H:%M:%S")

        # recreate tt with ACTUAL times injected
        gtfs_temp = pd.merge(
            tt,
            tt_rt_merged[["trip_id", "stop_sequence", "dt_arrival_time"]],
            on=["trip_id", "stop_sequence"],
        )

        # copy transpond times to arrival time and maintain axis location
        gtfs_temp["arrival_time"] = gtfs_temp["dt_arrival_time"]

        # aribtrary copying transpond times to departure time
        # (does this matter?)
        gtfs_temp["departure_time"] = gtfs_temp["dt_arrival_time"]

        gtfs_temp = gtfs_temp.drop(columns=["dt_arrival_time"])

        gtfs_temp = gtfs_temp.convert_dtypes({"route_type": "int"})

        gtfs_temp = gtfs_temp.sort_values(["trip_id", "stop_sequence"])

        return gtfs_temp

    def write_gtfs(self, gtfs_temp):
        """
        Write/export updated realtime/timetable data to individual GTFS files.

        Args:
            gtfs_temp (pandas_df): exploded timetable data with realtime times
        """

        from_dir = f"{self.dir}/timetable"
        to_dir = f"{self.dir}/realtime_gtfs"

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
                "trip_direction_name",
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
                "stop_direction_name",
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

        # n.b. hardcoded to tuesday
        # todo - infer active weekdays
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
        ] = (0, 1, 0, 0, 0, 0, 0, date, date)
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
            zip_name = f"{self.region}_{self.today}_realtimegtfs.zip"
            zip_files(to_dir, zip_name)

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
    config = toml.load("config.toml")["data_ingest"]
    builder = GTFS_Builder(config)

    logger.info("Loading all realtime data")
    real = builder.load_raw_realtime_data()

    labelled_real, unlabelled_real = builder.split_realtime_data(real)
    logger.info(f"Raw labelled realtime data: {len(labelled_real)} rows")
    logger.info(f"Raw UNLABELLED realtime data: {len(unlabelled_real)} rows")

    labelled_real = deduplicate(labelled_real)
    unlabelled_real = deduplicate(unlabelled_real)
    logger.info(f"Dedup labelled realtime data: {len(labelled_real)} rows")
    logger.info(f"Dedup UNLABELLED realtime data: {len(unlabelled_real)} rows")

    logger.info("Loading all timetable data")
    timetable = builder.load_raw_timetable_data()

    logger.info(
        "Extract timetable data aligned to realtime activity \
            - inject real times"
    )
    gtfs = builder.prepare_gtfs(labelled_real, timetable)

    logger.info("Write updated GTFS files")
    builder.write_gtfs(gtfs)

    if config["zip_gtfs"]:
        logger.info("Zipping GTFS files")

    logger.info("GTFS Builder complete....")
