from datetime import datetime
import logging
import toml
import os
from src.make_data.make_gtfs_from_real import GTFS_Builder
from src.utils.call_data_from_bucket import ingest_file_from_gcp
from src.utils.preprocessing import (
    apply_geography_label,
    convert_string_time_to_unix,
    build_stops,
)  # noqa: E501
from src.utils.resourcing import ingest_data_from_geoportal
import pandas as pd
import polars as pl
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
        boundaries: str = "data/LSOA_2021_boundaries.geojson",
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
        self.boundaries = boundaries
        self.gtfs = GTFS_Builder(**config["generic"], **config["data_ingest"])

        # Initialise logger
        if logger is None:
            self.logger = logging.getLogger(__name__)

        else:
            self.logger = logger

    def build_timetable(
        self, stops: pl.DataFrame, region: str
    ) -> pd.DataFrame:  # noqa: E501
        """Processes timetable for given region and day, slicing to
        specified time frame and applying unique identifier to
        each service stop"""

        df = self.gtfs.load_raw_timetable_data(stops, region)
        # # read as string but reformat float-like
        # values to integer-like values
        # df["route_id"] = df["route_id"].astype(str)
        # df["route_id"] = df["route_id"].replace(".0", "")

        df = df.to_pandas()
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

    def build_realtime(self, region: str) -> pl.DataFrame:
        """Processes realtime for given region and day.
        Applies unique identifier to each service stop"""

        if region is None:
            region = self.rt_region

        df = self.gtfs.load_raw_realtime_data(region)
        df, unlabelled = self.gtfs.split_realtime_data(df)
        df = df.sort(
            ["trip_id", "current_stop", "time_transpond", "time_ingest"]
        )  # noqa: E501

        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col("journey_date"),
                    pl.col("current_stop"),
                    pl.col("trip_id"),
                    pl.col("route_id"),
                ],
                separator="_",
            ).alias("UID")
        )
        df = df.unique("UID")

        return df, unlabelled

    # TODO: refactor to polars
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
    boundaries_endpoints = config["setup"]["boundaries_endpoints"]
    boundaries = config["schedules"]["boundaries"]
    query_params = config["setup"]["query_params"]

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

    logger.info("Building stops data from NaPTAN")
    stops = build_stops()

    # TODO: refactor to polars
    logger.info("Loading and building from raw timetable data")
    tt = pd.DataFrame()
    for region in config["schedules"]["tt_regions"]:
        logger.info(f"Processing timetable: {region}: {date}")
        tti = schedule_build.build_timetable(stops, region)
        tt = pd.concat([tt, tti])

    logger.info("Merging in geography labels to timetable")
    boundary_filename = f"data/{boundaries}"
    if not os.path.exists(boundary_filename):
        logger.info("Ingesting boundary data from ONS geoportal")
        ingest_data_from_geoportal(
            boundaries_endpoints[boundaries],
            query_params,
            filename=boundary_filename,  # noqa: E501
        )
    bounds = gpd.read_file(boundary_filename)
    # England LSOAs only
    bounds = bounds[bounds["LSOA21CD"].str[0] == "E"]
    tt = apply_geography_label(tt, bounds)

    # convert from geopandas to pandas to polars
    tt = pd.DataFrame(tt)

    # TODO:
    tt = pl.from_pandas(
        tt
    )  # pyarrow.lib.ArrowTypeError: Did not pass numpy.dtype object

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
    tt_lsoa_coverage = tt.groupby(pl.col("trip_id")).agg(pl.count())
    tt_lsoa_coverage = tt_lsoa_coverage.join(
        bounds[["LSOA21CD", "LSOA21NM"]], on="LSOA21CD", how="right"
    )

    tt_lsoa_coverage.write_csv(
        f"data/daily/metrics/tt_lsoa_coverage_{date}.csv"
    )  # noqa: E501

    logger.info("Loading and building from raw realtime data")
    rt = pl.DataFrame()
    rt_u = pl.DataFrame()
    for region in config["schedules"]["rt_regions"]:
        logger.info(f"Processing realtime: {region}: {date}")
        rti, rti_u = schedule_build.build_realtime(region=region)
        rti = rti.select(
            ["UID", "time_transpond", "bus_id", "latitude", "longitude"]
        )  # noqa: E501
        rt = pl.concat([rt, rti])
        rt_u = pl.concat([rt_u, rti_u])

    # temp conversion to pandas
    rt = rt.to_pandas()
    rt_u = rt_u.to_pandas()

    logger.info("Diagnostics: realtime coverage by LSOA")
    rt = apply_geography_label(rt, bounds, type="realtime")

    # temp conversion back to polars
    rt = pd.DataFrame(rt)
    rt = pl.from_pandas(rt)

    # illustrative output of realtime service stops by LSOA
    rt_lsoa_coverage = rt.groupby(pl.col("LSOA21CD")).agg(pl.count())
    rt_lsoa_coverage = rt_lsoa_coverage.join(
        bounds[["LSOA21CD", "LSOA21NM"]], on="LSOA21CD", how="right"
    )

    rt_lsoa_coverage.write_csv(
        f"data/daily/metrics/rt_lsoa_coverage_{date}.csv"
    )  # noqa: E501

    logger.info("Diagnostics: UNLABELLED realtime coverage by LSOA")

    # illustrative output of unlabelled RT rows by LSOA
    rt_u = apply_geography_label(rt_u, bounds, type="realtime")
    rt_u = pl.from_pandas(rt_u)

    if config["schedules"]["output_unlabelled_bulk"]:
        logger.info("Exporting unlabelled RT data to file")
        rt_u.write_csv("data/daily/unlabelled_rt.csv")
    else:
        logger.info("Unlabelled data export bypassed by user")

    rt_u_lsoa_coverage = rt_u.groupby(pl.col("LSOA21CD")).agg(pl.count())
    rt_u_lsoa_coverage = rt_u_lsoa_coverage.join(
        bounds[["LSOA21CD", "LSOA21NM"]], on="LSOA21CD", how="right"
    )

    rt_u_lsoa_coverage.write_csv(
        f"data/daily/metrics/rt_u_lsoa_coverage_{date}.csv"
    )  # noqa: E501

    logger.info("Writing schedule to file")
    # only service stops common in RT and TT
    schedule = (
        rt[["UID", "time_transpond", "bus_id"]]
        .join(tt, on="UID", how="inner")
        .unique()  # noqa: E501
    )

    schedule.write_csv(f"data/daily/schedules/schedule_england_{date}.csv")

    # temp conversion to pandas
    schedule = schedule.to_pandas()
    logger.info("Writing punctuality to file")
    punc = schedule_build.punctuality_by_lsoa(schedule)
    punc.to_csv(f"data/daily/metrics/punctuality_by_lsoa_england_{date}.csv")

    logger.info("\n")
    logger.info("-------------------------------")
    logger.info("Schedule Builder jobs completed")
    logger.info("-------------------------------")
