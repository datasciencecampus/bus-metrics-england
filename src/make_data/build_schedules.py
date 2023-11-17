from datetime import datetime
import logging
import toml
from src.make_data.make_gtfs_from_real import GTFS_Builder
from src.utils.preprocessing import (
    build_daily_stops_file,
    apply_geography_label_to_stops,
)
import pandas as pd
import geopandas as gpd
import numpy as np


class Schedule_Builder:
    """Auto-builds timetable and realtime data as a single schedule
    for all England on one single day. N.B. only service stops that
    are common in both timetable and realtime AND labelled correctly"""

    def __init__(self, config):
        self.tt_regions = config["schedules"]["tt_regions"]
        self.date = config["data_ingest"]["today"]
        self.start = config["schedules"]["partial_start"]
        self.end = config["schedules"]["partial_end"]
        logger: logging.Logger = (None,)

        self.gtfs = GTFS_Builder(config)

        # Initialise logger
        if logger is None:
            self.logger = logging.getLogger(__name__)

        else:
            self.logger = logger

        return None

    def build_timetable(
        self,
        region: str,
        date: int,
        time_from: str = "07:00:00",
        time_to: str = "10:00:00",
        partial_timetable: bool = False,
    ) -> pd.DataFrame:
        df = self.gtfs.load_raw_timetable_data(region=region, date=date)
        # read as string but reformat float-like values to integer-like values
        df["route_id"] = df["route_id"].astype(str)
        df["route_id"] = df["route_id"].replace(".0", "")

        if partial_timetable:
            start = datetime.strptime(
                str(date) + " " + str(time_from), "%Y%m%d %H:%M:%S"
            )
            start = datetime.timestamp(start)
            finish = datetime.strptime(
                str(date) + " " + str(time_to), "%Y%m%d %H:%M:%S"
            )
            finish = datetime.timestamp(finish)
            df = df[
                (df["unix_arrival_time"] >= start)
                & (df["unix_arrival_time"] < finish)  # noqa: E501
            ]  # noqa: E501

        df["UID"] = (
            df["timetable_date"].astype(str)
            + "_"
            + df["stop_sequence"].astype(str)
            + "_"
            + df["trip_id"].astype(str)
            + "_"
            + df["route_id"].astype(str)
        )

        return df

    def build_realtime(self, region: str, date: int) -> pd.DataFrame:
        df = self.gtfs.load_raw_realtime_data(region=region, date=date)
        # # read as string but reformat float-like values
        # to integer-like values
        # df["route_id"] = df["route_id"].astype(str)
        # df["route_id"] = df["route_id"].replace(".0", "")

        df, _ = self.gtfs.split_realtime_data(df)
        df = df.sort_values(
            ["trip_id", "current_stop", "time_transpond", "time_ingest"]
        )

        df["UID"] = (
            df["journey_date"].astype(str)
            + "_"
            + df["current_stop"].astype(str)
            + "_"
            + df["trip_id"].astype(str)
            + "_"
            + df["route_id"].astype(str)
        )
        df = df.drop_duplicates(["UID"])

        return df

    def punctuality_by_lsoa(
        self, df: pd.DataFrame, geog_no_stops: list
    ) -> pd.DataFrame:
        df["unix_arrival_time"] = (
            df["unix_arrival_time"] + 3600
        )  # dealing with GMT date
        df["relative_punctuality"] = (
            df["unix_arrival_time"] - df["time_transpond"]
        )  # noqa: E501
        df["punctual"] = np.where(
            (df["relative_punctuality"] > -300)
            & (df["relative_punctuality"] < 60),  # noqa: E501
            1,
            0,
        )
        df = (
            df.groupby("LSOA21CD")["punctual"]
            .agg(["count", "mean"])
            .reset_index()  # noqa: E501
        )  # noqa: E501
        geog_no_stops_vals = [[x, 0, np.nan] for x in geog_no_stops]
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    geog_no_stops_vals, columns=["LSOA21CD", "count", "mean"]
                ),  # noqa: E501
            ]
        )
        return df


if __name__ == "__main__":
    # define session_id that will be used for log file and feedback
    session_name = (
        f"schedule_builder_{format(datetime.now(), '%Y_%m_%d_%H:%M')}"  # noqa: E501
    )
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
    gtfs_build = GTFS_Builder(toml.load("config.toml"))
    schedule_build = Schedule_Builder(toml.load("config.toml"))

    tt_regions = config["schedules"]["tt_regions"]
    rt_regions = config["schedules"]["rt_regions"]
    date = config["data_ingest"]["today"]

    logger.info("Preparing geography labels")
    # stops = pd.read_csv("data/daily/gb_stops.csv")     # NAPTAN data
    # stops = stops[["ATCOCode", "Latitude", "Longitude"]]
    # stops.columns = ["stop_id", "stop_lat", "stop_lon"]
    stops = build_daily_stops_file(date)
    stops.to_csv(f"data/daily/stops_{date}.txt")
    logger.info(
        f"PRE-LABEL.... Stop rows: {len(stops)}..... Unique stop ids: {stops['stop_id'].nunique()}"  # noqa: E501
    )
    bounds = gpd.read_file("data/LSOA_2021_boundaries.geojson")
    stops_lab = apply_geography_label_to_stops(stops, bounds)
    stops_lab = stops_lab.reset_index(drop=True)
    logger.info(
        f"POST-LABEL.... Stop rows: {len(stops_lab)}..... Unique stop ids: {stops_lab['stop_id'].nunique()}"  # noqa: E501
    )
    logger.info(f"LSOAs captured: {stops_lab['LSOA21CD'].nunique()}")

    # LSOAs containing no physical bus stops
    lsoas_no_stops = list(set(bounds["LSOA21CD"]) - set(stops_lab["LSOA21CD"]))
    lsoas_no_stops = [x for x in lsoas_no_stops if x[0] == "E"]

    logger.info("Loading and building from raw timetable data")
    tt = pd.DataFrame()
    for region in tt_regions:
        logger.info(f"Processing timetable: {region}: {date}")
        tti = schedule_build.build_timetable(
            region=region,
            date=date,
            time_from="06:30:00",
            time_to="10:30:00",
            partial_timetable=True,
        )

        tt = pd.concat([tt, tti])

    logger.info("Merging in geography labels to timetable")
    tt = pd.merge(tt, stops_lab, how="left", on="stop_id")
    tt = tt[
        [
            "UID",
            "unix_arrival_time",
            "trip_id",
            "route_id",
            "service_id",
            "stop_sequence",
            "trip_headsign",
            "stop_id",
            "LSOA21CD",
            "LSOA21NM",
        ]
    ]

    logger.info("Loading and building from raw realtime data")
    rt = pd.DataFrame()
    for region in rt_regions:
        logger.info(f"Processing realtime: {region}: {date}")
        rti = schedule_build.build_realtime(region=region, date=date)
        rti = rti[["UID", "time_transpond", "bus_id"]]
        rt = pd.concat([rt, rti])

    logger.info("Writing schedule to file")
    # only service stops common in RT and TT
    schedule = pd.merge(rt, tt, on="UID", how="inner")
    schedule.to_csv(f"data/daily/schedules/schedule_england_{date}.csv")

    lost_ids = list(set(schedule["stop_id"]) - set(stops_lab["stop_id"]))
    logger.info(f"Stop ids in the schedule, not in stops.txt: {len(lost_ids)}")
    logger.info(lost_ids[:10])

    logger.info("Writing punctuality to file")
    punc = schedule_build.punctuality_by_lsoa(schedule, lsoas_no_stops)
    punc.to_csv(f"data/daily/schedules/punctuality_by_lsoa_england_{date}.csv")

    logger.info("\n")
    logger.info("-------------------------------")
    logger.info("Schedule Builder jobs completed")
    logger.info("-------------------------------")
