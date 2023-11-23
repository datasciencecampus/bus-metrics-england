from datetime import datetime
import logging
import toml
from src.make_data.make_gtfs_from_real import GTFS_Builder
from src.utils.preprocessing import apply_geography_label
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
        """Processes timetable for given region and day, slicing to
        specified time frame and applying unique identifier to
        each service stop"""

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
        """Processes realtime for given region and day.
        Applies unique identifier to each service stop"""
        df = self.gtfs.load_raw_realtime_data(region=region, date=date)
        # # read as string but reformat float-like values
        # to integer-like values
        # df["route_id"] = df["route_id"].astype(str)
        # df["route_id"] = df["route_id"].replace(".0", "")

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
    for region in rt_regions:
        logger.info(f"Processing realtime: {region}: {date}")
        rti, rti_u = schedule_build.build_realtime(region=region, date=date)
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
    punc.to_csv(f"data/daily/schedules/punctuality_by_lsoa_england_{date}.csv")

    logger.info("\n")
    logger.info("-------------------------------")
    logger.info("Schedule Builder jobs completed")
    logger.info("-------------------------------")
