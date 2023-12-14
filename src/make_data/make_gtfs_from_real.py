import logging
from datetime import datetime
import toml
import os
from os import listdir
import glob
import pandas as pd
import polars as pl
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

    def load_raw_realtime_data(self, region=None) -> pl.DataFrame:
        """
        Collects all realtime data for individual day

        Args:
            region (str; optional): region name

        Returns:
            df (polars df): unprocessed realtime data
        """

        if region is None:
            region = self.rt_region

        dir = f"data/daily/realtime/{region}/{self.date}/"
        # collate all realtime ingests to single dataframe
        tables = os.path.join(dir, "*.csv")
        tables = glob.glob(tables)
        df_list = (pl.read_csv(table, ignore_errors=True) for table in tables)
        df = pl.concat(df_list)

        return df

    def split_realtime_data(
        self, df: pl.DataFrame
    ) -> (pl.DataFrame, pl.DataFrame):  # noqa: E501
        """
        Collects and concatenates all realtime data for specified day

        Args:
            df (polars df): unprocessed realtime data

        Returns:
            labelled_real (polars df): rows with trip_id & route_id
            unlabelled_real (polars df): row with trip_id & route_id MISSING
        """

        labelled_real = df.filter(
            (pl.col("trip_id").is_not_null())
            & (pl.col("route_id").is_not_null())  # noqa: E501
        )
        unlabelled_real = df.filter(
            (pl.col("trip_id").is_null()) & (pl.col("route_id").is_null())
        )

        return labelled_real, unlabelled_real

    def load_raw_timetable_data(
        self, stops: pl.DataFrame, region: str = None
    ) -> pl.DataFrame:
        """
        Collects and concatenates all timetable data for specified
        day, returning all individual service stops (every 'bus
        at stop' activity) for the day.

        Args:
            stops (polars df): NAPTAN stops data
            region (str; optional): region name

        Returns:
            service_stops (polars df): all service stops
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
                logger=self.logger,
            )

        calendar_dates = pl.read_csv(
            f"{from_dir}/calendar_dates.txt", ignore_errors=True
        )
        calendar = pl.read_csv(f"{from_dir}/calendar.txt", ignore_errors=True)
        routes = pl.read_csv(f"{from_dir}/routes.txt", ignore_errors=True)
        stop_times = pl.read_csv(
            f"{from_dir}/stop_times.txt",
            ignore_errors=True,
            dtypes={"stop_id": pl.Utf8},
        )

        trips = pl.read_csv(f"{from_dir}/trips.txt", ignore_errors=True)

        calendar_dates = calendar_dates.filter(
            pl.col("date").cast(pl.Utf8) == self.date
        )
        exception_drops = calendar_dates.filter(
            pl.col("exception_type") == 2
        ).select(  # noqa: E501
            pl.col("service_id")
        )
        exception_adds = calendar_dates.filter(pl.col("exception_type") == 1)[
            "service_id"
        ].to_list()

        # filter routes to required route_type(s)
        routes = routes.filter(pl.col("route_type").is_in(self.route_types))

        service_stops = (
            trips.join(stop_times, on="trip_id")
            .join(stops, on="stop_id", how="left")
            .join(routes, on="route_id", how="left")
        )

        # drop & add exceptions
        calendar = calendar.join(exception_drops, on="service_id", how="anti")
        active_services = calendar.filter(
            pl.col(self.weekday)
            == 1 | pl.col("service_id").is_in(exception_adds)  # noqa: E501
        )["service_id"].to_list()
        service_stops = service_stops.filter(
            pl.col("service_id").is_in(active_services)
        )
        service_stops = service_stops.drop_nulls(subset="route_type")
        service_stops = service_stops.with_columns(
            pl.lit(self.date).alias("timetable_date")
        )
        service_stops = service_stops.filter(
            pl.col("arrival_time").str.slice(0, 2).cast(pl.UInt32) < 24
        )

        # temp convert from polars to pandas
        service_stops = service_stops.to_pandas()

        service_stops = convert_string_time_to_unix(
            self.date, service_stops, "arrival_time", convert_type="column"
        )  # noqa: E501

        # temp convert from pandas to polars
        service_stops = pl.from_pandas(service_stops)

        return service_stops

    def prepare_gtfs(
        self, labelled_real: pl.DataFrame, timetable: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Extracts all timetable rows aligned to trip_id active in realtime data,
        replacing timetabled times with realtime times.

        Args:
            labelled_real (polars df): rows with trip_id & route_id
            timetable (polars df): exploded timetable data

        Returns:
            gtfs_temp (polars df): exploded timetable data with realtime times
        """
        trip_ids = set(labelled_real["trip_id"].drop_nans().to_list())

        # filter timetable to only RT trip_ids
        tt = timetable.filter(pl.col("trip_id").is_in(trip_ids))

        tt_rt_merged = tt[["timetable_date", "trip_id", "stop_sequence"]].join(
            labelled_real[["time_transpond", "trip_id", "current_stop"]],
            left_on=["trip_id", "stop_sequence"],
            right_on=["trip_id", "current_stop"],
        )

        # temp convert from polars to pandas
        tt_rt_merged = tt_rt_merged.to_pandas()

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

        # temp convert from pandas to polars
        tt_rt_merged = pl.from_pandas(tt_rt_merged)

        # recreate tt with ACTUAL times injected
        gtfs_temp = tt.join(
            tt_rt_merged[["trip_id", "stop_sequence", "dt_time_transpond"]],
            on=["trip_id", "stop_sequence"],
        )

        gtfs_temp = gtfs_temp.rename({"dt_time_transpond": "arrival_time"})
        gtfs_temp["departure_time"] = gtfs_temp["arrival_time"]
        gtfs_temp = gtfs_temp.sort(["trip_id", "stop_sequence"])

        return gtfs_temp

    def write_gtfs(self, gtfs_temp: pl.DataFrame) -> None:
        """
        Write/export updated realtime/timetable data to individual GTFS files.

        Args:
            gtfs_temp (polars_df): exploded timetable data with realtime times
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
                "trip_direction_name",
                "vehicle_journey_code",
            ]
        ].unique().write_csv(f"{to_dir}/trips.txt")

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
        ].unique().write_csv(f"{to_dir}/stop_times.txt")

        # reverse-engineered stop_times.txt
        gtfs_temp[
            [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_type",
            ]
        ].unique().write_csv(
            f"{to_dir}/routes.txt"
        )  # noqa: E501

        # empty file as all exceptions accounted for
        cal_dates = pl.DataFrame(
            data=None,
            schema={
                "service_id": pl.Utf8,
                "date": pl.Utf8,
                "exception_type": pl.Int32,
            },  # noqa: E501
        )  # noqa: E501
        cal_dates.write_csv(f"{to_dir}/calendar_dates.txt")

        calendar = pl.read_csv(f"{from_dir}/calendar.txt", ignore_errors=True)
        calendar = gtfs_temp["service_id"].join(
            calendar, on="service_id", how="left"
        )  # noqa: E501
        calendar[["start_date", "end_date"]] = self.date

        calendar.unique().write_csv(f"{to_dir}/calendar.txt")

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

    # # temp conversion to from polars to pandas
    # labelled_real = labelled_real.to_pandas()
    # unlabelled_real = unlabelled_real.to_pandas()

    logger.info(f"Raw labelled realtime data: {len(labelled_real)} rows")
    logger.info(f"Raw UNLABELLED realtime data: {len(unlabelled_real)} rows")

    labelled_real = deduplicate(labelled_real)
    unlabelled_real = deduplicate(unlabelled_real)
    logger.info(f"Dedup labelled realtime data: {len(labelled_real)} rows")
    logger.info(f"Dedup UNLABELLED realtime data: {len(unlabelled_real)} rows")

    logger.info("Building stops data from NaPTAN")
    stops = build_stops()

    logger.info("Loading all timetable data")
    timetable = builder.load_raw_timetable_data(stops=stops)

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
