from datetime import datetime
import logging
import toml
from src.make_data.make_gtfs_from_real import GTFS_Builder
from src.utils.call_data_from_bucket import ingest_file_from_gcp
from src.utils.preprocessing import (
    apply_geography_label,
    convert_string_time_to_unix,
)  # noqa: E501
import pandas as pd
import geopandas as gpd
import numpy as np


class Schedule_Builder:
    """Auto-builds timetable and realtime data as a single schedule
    for all England on one single day. N.B. only service stops that
    are common in both timetable and realtime AND labelled correctly"""

    def __init__(
        self,
        tt_regions: list = [
            "EastAnglia",
            "EastMidlands",
            "NorthEast",
            "NorthWest",
            "SouthEast",
            "SouthWest",
            "WestMidlands",
            "Yorkshire",
        ],
        rt_regions: list = [
            "EastMidlands",
            "EastofEngland",
            "NorthEast",
            "NorthWest",
            "SouthEast",
            "SouthWest",
            "WestMidlands",
            "YorkshireandTheHumber",
        ],
        region: str = "YorkshireandTheHumber",
        date: str = "20231103",
        time_from: float = 7.0,
        time_to: float = 10.0,
        partial_timetable: bool = True,
        output_unlabelled_bulk: bool = False,
        logger: logging.Logger = (None,),
    ):
        self.tt_regions = tt_regions
        self.rt_regions = rt_regions
        self.region = region
        self.date = date
        self.time_from = time_from
        self.time_to = time_to
        self.partial_timetable = partial_timetable
        self.output_unlabelled_bulk = output_unlabelled_bulk
        self.gtfs = GTFS_Builder(**config["generic"], **config["data_ingest"])

        # Initialise logger
        if logger is None:
            self.logger = logging.getLogger(__name__)

        else:
            self.logger = logger

    def build_timetable(self, region: str) -> pd.DataFrame:
        """Processes timetable for given region and day, slicing to
        specified time frame and applying unique identifier to
        each service stop"""

        df = self.gtfs.load_raw_timetable_data(region)
        # read as string but reformat float-like values to integer-like values
        df["route_id"] = df["route_id"].astype(str)
        df["route_id"] = df["route_id"].replace(".0", "")

        # slicing timetable with 30 minute buffers either side of
        # realtime window
        datestamp = convert_string_time_to_unix(
            date=self.date, convert_type="single"
        )  # noqa: E501
        tt_time_from = int(datestamp + (self.time_from * 60 * 60) - 1800)
        tt_time_to = int(datestamp + (self.time_to * 60 * 60) + 1800)

        if self.partial_timetable:
            df = df[
                (df["unix_arrival_time"] >= tt_time_from)
                & (df["unix_arrival_time"] < tt_time_to)
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

    def build_realtime(self, region: str) -> pd.DataFrame:
        """Processes realtime for given region and day.
        Applies unique identifier to each service stop"""

        if region is None:
            region = self.rt_region

        df = self.gtfs.load_raw_realtime_data(region)

        df, unlabelled = self.gtfs.split_realtime_data(df)
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

        return df, unlabelled

    def punctuality_by_lsoa(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies punctuality and punctuality flag to
        RT/TT combined schedule"""
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
    gtfs_build = GTFS_Builder(**config["generic"], **config["data_ingest"])
    schedule_build = Schedule_Builder(
        **config["generic"], **config["schedules"]
    )  # noqa: E501
    date = config["generic"]["date"]

    for tt_region in config["schedules"]["tt_regions"]:
        ingest_file_from_gcp(
            logger=logger,
            region=tt_region,
            date=date,
            download_type="timetable",
        )

    for rt_region in config["schedules"]["rt_regions"]:
        ingest_file_from_gcp(
            logger=logger,
            region=rt_region,
            date=date,
            download_type="realtime",
        )

    logger.info("Loading and building from raw timetable data")
    tt = pd.DataFrame()
    for region in config["schedules"]["tt_regions"]:
        logger.info(f"Processing timetable: {region}: {date}")
        tti = schedule_build.build_timetable(region)
        tt = pd.concat([tt, tti])

    logger.info("Merging in geography labels to timetable")
    bounds = gpd.read_file("data/LSOA_2021_boundaries.geojson")
    # England LSOAs only
    bounds = bounds[bounds["LSOA21CD"].str[0] == "E"]
    tt = apply_geography_label(tt, bounds)
    tt = tt.reset_index(drop=True)

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
            "stop_lat",
            "stop_lon",
        ]
    ]

    logger.info("Diagnostics: timetable coverage by LSOA")
    # illustrative output of timetabled service stops by LSOA
    tt_lsoa_coverage = pd.DataFrame(
        tt["LSOA21CD"].value_counts()
    ).reset_index()  # noqa: E501
    tt_lsoa_coverage = pd.merge(
        bounds[["LSOA21CD", "LSOA21NM"]],
        tt_lsoa_coverage,
        on="LSOA21CD",
        how="left",  # noqa: E501
    )
    tt_lsoa_coverage.to_csv(f"data/daily/metrics/tt_lsoa_coverage_{date}.csv")

    logger.info("Loading and building from raw realtime data")
    rt = pd.DataFrame()
    rt_u = pd.DataFrame()
    for region in config["schedules"]["rt_regions"]:
        logger.info(f"Processing realtime: {region}: {date}")
        rti, rti_u = schedule_build.build_realtime(region=region)
        rti = rti[["UID", "time_transpond", "bus_id", "latitude", "longitude"]]
        rt = pd.concat([rt, rti])
        rt_u = pd.concat([rt_u, rti_u])

    logger.info("Diagnostics: realtime coverage by LSOA")
    rt = apply_geography_label(rt, bounds, type="realtime")
    rt = rt.reset_index(drop=True)

    # illustrative output of realtime service stops by LSOA
    rt_lsoa_coverage = pd.DataFrame(
        rt["LSOA21CD"].value_counts()
    ).reset_index()  # noqa: E501

    rt_lsoa_coverage = pd.merge(
        bounds[["LSOA21CD", "LSOA21NM"]],
        rt_lsoa_coverage,
        on="LSOA21CD",
        how="left",  # noqa: E501
    )
    rt_lsoa_coverage.to_csv(f"data/daily/metrics/rt_lsoa_coverage_{date}.csv")

    logger.info("Diagnostics: UNLABELLED realtime coverage by LSOA")

    # illustrative output of unlabelled RT rows by LSOA
    rt_u = apply_geography_label(rt_u, bounds, type="realtime")
    rt_u = rt_u.reset_index(drop=True)

    if config["schedules"]["output_unlabelled_bulk"]:
        logger.info("Exporting unlabelled RT data to file")
        rt_u.to_csv("data/daily/unlabelled_rt.csv")
    else:
        logger.info("Unlabelled data export bypassed by user")

    rt_u_lsoa_coverage = pd.DataFrame(
        rt_u["LSOA21CD"].value_counts()
    ).reset_index()  # noqa: E501
    rt_u_lsoa_coverage = pd.merge(
        bounds[["LSOA21CD", "LSOA21NM"]],
        rt_u_lsoa_coverage,
        on="LSOA21CD",
        how="left",  # noqa: E501
    )

    rt_u_lsoa_coverage.to_csv(
        f"data/daily/metrics/rt_u_lsoa_coverage_{date}.csv"
    )  # noqa: E501

    logger.info("Writing schedule to file")
    # only service stops common in RT and TT
    schedule = pd.merge(
        rt[["UID", "time_transpond", "bus_id"]], tt, on="UID", how="inner"
    )
    schedule = schedule.drop_duplicates()  # TODO: check if req'd
    schedule.to_csv(f"data/daily/schedules/schedule_england_{date}.csv")

    logger.info("Writing punctuality to file")
    punc = schedule_build.punctuality_by_lsoa(schedule)
    punc.to_csv(f"data/daily/metrics/punctuality_by_lsoa_england_{date}.csv")

    logger.info("\n")
    logger.info("-------------------------------")
    logger.info("Schedule Builder jobs completed")
    logger.info("-------------------------------")
