from datetime import datetime
import logging
import toml
import os
from src.make_data.make_gtfs_from_real import GTFS_Builder
from src.utils.call_data_from_bucket import ingest_file_from_gcp
from src.utils.preprocessing import (
    apply_geography_label,
    convert_SINGLE_datetime_to_unix,
)
from src.utils.resourcing import ingest_data_from_geoportal
import polars as pl
import geopandas as gpd


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
    ) -> pl.DataFrame:
        """Processes timetable for given region and day, slicing to
        specified time frame and applying unique identifier to
        each service stop"""

        df = self.gtfs.load_raw_timetable_data(stops, region)
        # slicing timetable with 30 minute buffers either side of
        # realtime window
        datestamp = convert_SINGLE_datetime_to_unix(date=self.date)
        tt_time_from = int(datestamp + (self.time_from * 60 * 60) - 1800)
        tt_time_to = int(datestamp + (self.time_to * 60 * 60) + 1800)
        if self.partial_timetable:
            df = df.with_columns(
                (pl.col("unix_arrival_time") >= tt_time_from)
                & (pl.col("unix_arrival_time") < tt_time_to)
            )
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col("timetable_date"),
                    pl.col("stop_sequence"),
                    pl.col("trip_id"),
                    pl.col("route_id"),
                ],
                separator="_",
            ).alias("UID")
        )
        df = df.unique("UID")

        return df

    def build_realtime(self, region: str) -> (pl.DataFrame, pl.DataFrame):
        """Processes realtime for given region and day.
        Applies unique identifier to each service stop"""

        if region is None:
            region = self.rt_region
        df = self.gtfs.load_raw_realtime_data(region)
        df, unlabelled = self.gtfs.split_realtime_data(df)
        df = df.sort(
            ["trip_id", "current_stop", "time_transpond", "time_ingest"]
        )
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

    def punctuality_by_lsoa(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies punctuality and punctuality flag to
        RT/TT combined schedule"""

        df = df.with_columns(
            (pl.col("unix_arrival_time") - pl.col("time_transpond")).alias(
                "relative_punctuality"
            )
        )
        df = df.with_columns(
            pl.when(
                (pl.col("relative_punctuality") > -300)
                | (pl.col("relative_punctuality") < 60)
            )
            .then(1)
            .otherwise(0)
            .alias("punctual")
        )
        df = df.group_by("LSOA21CD").agg(
            [
                pl.count("punctual").alias("num_service_stops"),
                pl.mean("punctual").alias("avg_punctuality"),
            ]
        )

        return df


if __name__ == "__main__":
    # define session_id that will be used for log file and feedback
    session_name = (
        f"schedule_builder_{format(datetime.now(), '%Y_%m_%d_%H:%M')}"
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
    )
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

    logger.info("Loading NAPTAN-geography lookup table")
    stops = pl.read_csv("data/daily/gb_stops_labelled.csv")

    logger.info("Loading and building from raw timetable data")
    tt_cols = [
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
    trigger = 0
    for region in config["schedules"]["tt_regions"]:
        logger.info(f"Processing timetable: {region}: {date}")
        tti = schedule_build.build_timetable(stops, region)
        tti = tti[tt_cols]
        if trigger == 0:
            tt = tti.clone()
        else:
            tt = pl.concat([tt, tti], how="vertical_relaxed")
        trigger += 1

    boundary_filename = f"data/{boundaries}"
    if not os.path.exists(boundary_filename):
        logger.info("Ingesting boundary data from ONS geoportal")
        ingest_data_from_geoportal(
            boundaries_endpoints[boundaries],
            query_params,
            filename=boundary_filename,
        )

    logger.info("Loading boundary data to memory")
    bounds = gpd.read_file(boundary_filename)
    # England LSOAs only
    bounds = bounds[bounds["LSOA21CD"].str[0] == "E"]
    bounds_lookup = bounds[["LSOA21CD", "LSOA21NM"]]
    bounds_lookup = pl.from_pandas(bounds_lookup)
    logger.info("Diagnostics: timetable coverage by LSOA")
    # illustrative output of timetabled service stops by LSOA

    # TODO: can boundaries geojson be loaded to polars?
    tt_lsoa_coverage = bounds_lookup.join(
        (tt.group_by("LSOA21CD").count()),
        on="LSOA21CD",
        how="left",
    )

    tt_lsoa_coverage.write_csv(
        f"data/daily/metrics/tt_lsoa_coverage_{date}.csv"
    )

    logger.info("Loading and building from raw realtime data")
    trigger = 0
    for region in config["schedules"]["rt_regions"]:
        logger.info(f"Processing realtime: {region}: {date}")
        rti, rti_u = schedule_build.build_realtime(region=region)
        rti = rti.select(
            ["UID", "time_transpond", "bus_id", "latitude", "longitude"]
        )
        if trigger == 0:
            rt = rti.clone()
            rt_u = rti_u.clone()
        else:
            rt = pl.concat([rt, rti], how="vertical_relaxed")
            rt_u = pl.concat([rt_u, rti_u], how="vertical_relaxed")
        trigger += 1

    # temp conversion to pandas
    rt = rt.to_pandas()
    rt_u = rt_u.to_pandas()

    logger.info("Applying geography labels to realtime (labelled) data")
    rt = apply_geography_label(rt, bounds, type="realtime")

    # temp conversion back to polars
    rt = pl.from_pandas(rt)

    logger.info("Diagnostics: realtime coverage by LSOA")
    rt_lsoa_coverage = bounds_lookup.join(
        (rt.group_by("LSOA21CD").count()),
        on="LSOA21CD",
        how="left",
    )

    logger.info("Writing realtime (labelled) coverage to file")
    rt_lsoa_coverage.write_csv(
        f"data/daily/metrics/rt_lsoa_coverage_{date}.csv"
    )

    if config["schedules"]["output_unlabelled_bulk"]:
        # temp bypass unlabelled processing
        # illustrative output of unlabelled RT rows by LSOA
        logger.info("Applying geography labels to realtime (UNLABELLED) data")
        rt_u = apply_geography_label(rt_u, bounds, type="realtime")

        # temp conversion back to polars
        rt_u = pl.from_pandas(rt_u)

        logger.info("Exporting unlabelled RT data to file")
        rt_u.write_csv("data/daily/unlabelled_rt.csv")

        logger.info("Diagnostics: realtime (UNLABELLED) coverage by LSOA")
        rt_u_lsoa_coverage = bounds_lookup.join(
            (rt_u.group_by("LSOA21CD").count()),
            on="LSOA21CD",
            how="left",
        )

        logger.info("Writing realtime (UNLABELLED) coverage to file")
        rt_u_lsoa_coverage.write_csv(
            f"data/daily/metrics/rt_u_lsoa_coverage_{date}.csv"
        )
    else:
        logger.info("Unlabelled data export and analysis bypassed by user")

    logger.info("Writing schedule to file")
    # only service stops common in RT and TT
    schedule = (
        rt[["UID", "time_transpond", "bus_id"]]
        .join(tt, on="UID", how="inner")
        .unique()
    )

    schedule.write_csv(f"data/daily/schedules/schedule_england_{date}.csv")

    logger.info("Writing punctuality to file")
    punc = schedule_build.punctuality_by_lsoa(schedule)
    punc.write_csv(
        f"data/daily/metrics/punctuality_by_lsoa_england_{date}.csv"
    )

    logger.info("\n")
    logger.info("-------------------------------")
    logger.info("Schedule Builder jobs completed")
    logger.info("-------------------------------")
