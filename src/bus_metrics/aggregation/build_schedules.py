"""Class to create a region wide stops reliability metric."""

from datetime import datetime
import logging
import toml
import os
import glob
from src.bus_metrics.aggregation.preprocessing import (
    build_stops,
    convert_SINGLE_datetime_to_unix,
    polars_robust_load_csv,
    unzip_GTFS,
    convert_string_time_to_unix,
)
import polars as pl
import argparse


class Schedule_Builder:
    """Class to load data and create metrics.

    Auto-builds timetable and realtime data as a single schedule
    for single English region on one single day. N.B. only service stops that
    are common in both timetable and realtime AND labelled correctly.

    Parameters
    ----------
    region : str, optional
        Region (Bounding Box) to read in realtime data and calculate metrics.
    date : str, optional
        Date of day in which to calculate metrics.
    time_from : float, optional
        Start time in which to filter timetable and real time data to.
    time_to : float, optional
        End time in which to filter timetable and real time data to.
    partial_timetable : bool, optional
        Flag to use `time_from` and `time_to` to filter data.
    route_types : list, optional
        List of route codes to include in metric calculation e.g. [3] for bus.
    output_unlabelled_bulk : bool, optional
        Option to save unlabelled real time data if ingested.
    logger : loggin.logger, optional
        Logger instance to save progress messages.

    """

    def __init__(
        self,
        date: str = datetime.today().strftime("%Y%m%d"),
        region: str = "north_east",
        time_from: float = 7.0,
        time_to: float = 10.0,
        partial_timetable: bool = False,
        route_types: list = [3],
        output_unlabelled_bulk: bool = False,
        logger: logging.Logger = None,
    ):
        self.region = region
        self.date = date
        self.time_from = time_from
        self.time_to = time_to
        self.partial_timetable = partial_timetable
        self.route_types = route_types
        self.output_unlabelled_bulk = output_unlabelled_bulk
        self.logger = logger
        self.timetable_dir = "data/timetable"
        self.realtime_dir = "data/realtime"

    def load_raw_timetable_data(
        self,
        stops: pl.DataFrame,
        region: str = None,
        date: str = None,
    ) -> pl.DataFrame:
        """Load timetable data.

        Collect and concatenates all timetable data for specified
        day, returning all individual service stops (every 'bus
        at stop' activity) for the day.

        Parameters
        ----------
        stops : polars.DataFrame
            NAPTAN stops data.
        region : str, optional
            Region name.
        date : str, optional
            Date in string format %Y%m%d.

        Returns
        -------
        service_stops : polars.DataFrame
            All service stops.

        """
        if region is None:
            region = self.region

        if date is None:
            date = self.date

        weekday = datetime.strptime(date, "%Y%m%d").strftime("%A").lower()
        from_dir = self.timetable_dir

        unzip_GTFS(
            txt_path=from_dir,
            zip_path=from_dir,
            file_name_pattern=f"{region}_{date}.zip",
            logger=self.logger,
        )

        calendar_dates = pl.read_csv(
            f"{from_dir}/calendar_dates.txt", ignore_errors=True
        )
        calendar = pl.read_csv(f"{from_dir}/calendar.txt", ignore_errors=True)
        routes = polars_robust_load_csv(
            f"{from_dir}/routes.txt", dtypes={"route_id": pl.Utf8}
        )
        stop_times = polars_robust_load_csv(
            f"{from_dir}/stop_times.txt",
            dtypes={"stop_id": pl.Utf8},
        )

        trips = polars_robust_load_csv(
            f"{from_dir}/trips.txt", dtypes={"route_id": pl.Utf8}
        )

        calendar_dates = calendar_dates.filter(
            pl.col("date").cast(pl.Utf8) == date
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

        # filter to today's activity plus added exceptions
        active_services = calendar.filter(pl.col(weekday) == 1)[
            "service_id"
        ].to_list()
        active_services.extend(exception_adds)
        trips = trips.filter(pl.col("service_id").is_in(active_services))
        trips = trips.join(exception_drops, on="service_id", how="anti")

        # drop cancelled services
        service_stops = (
            trips.join(stop_times, on="trip_id")
            .join(stops, on="stop_id", how="left")
            .join(routes, on="route_id", how="left")
        )

        # dropping rows with missing values attributed to
        # route_id or route_type
        service_stops = service_stops.drop_nulls(
            subset=["route_id", "route_type"]
        )
        # add timetable date column
        service_stops = service_stops.with_columns(
            pl.lit(date).alias("timetable_date")
        )
        # drop all times beyond 24 hour clock
        # TODO: can these be handled better?
        service_stops = service_stops.filter(
            pl.col("arrival_time").str.slice(0, 2).cast(pl.UInt32) < 24
        )

        service_stops = convert_string_time_to_unix(
            service_stops, "arrival_time"
        )

        return service_stops

    def load_raw_realtime_data(self, region=None, date=None) -> pl.DataFrame:
        """Collect all realtime data for individual day.

        Parameters
        ----------
        region : str, optional
            Region name.
        date : str, optional
            Date in string format %Y%m%d.

        Returns
        -------
        df : polars.DataFrame
            Unprocessed realtime data.

        """
        if region is None:
            region = self.region

        if date is None:
            date = self.date

        dir = self.realtime_dir

        # collate all realtime ingests to single dataframe
        tables = os.path.join(dir, f"{region}_{date}*.csv")
        tables = glob.glob(tables)
        df_list = [
            polars_robust_load_csv(table, dtypes={"route_id": pl.Utf8})
            for table in tables
        ]
        df = pl.concat(df_list)

        return df

    def split_realtime_data(
        self, df: pl.DataFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Collect and concatenates all realtime data for specified day.

        Parameters
        ----------
        df : polars.DataFrame
            Unprocessed realtime data.

        Returns
        -------
        labelled_real : polars.DataFrame
            Rows with trip_id & route_id.
        unlabelled_real (polars df):
            Row with trip_id & route_id MISSING (currently unused).

        """
        labelled_real = df.filter(
            (pl.col("trip_id").is_not_null())
            & (pl.col("route_id").is_not_null())
        )
        unlabelled_real = df.filter(
            (pl.col("trip_id").is_null()) & (pl.col("route_id").is_null())
        )

        return labelled_real, unlabelled_real

    def build_timetable(
        self, stops: pl.DataFrame, region: str, date: str
    ) -> pl.DataFrame:
        """Load and create day timetable.

        Processes timetable for given region and day, slicing to
        specified time frame and applying unique identifier to
        each service stop.

        Parameters
        ----------
        stops : polars.DataFrame
            Dataframe containing all stops information.
        region: str
            Region name to process.
        date : str
            Date in string format %Y%m%d.

        Returns
        -------
        df : polars.DataFrame
            Datframe of region timetable.

        """
        df = self.load_raw_timetable_data(stops, region)

        # slicing timetable with 30 minute buffers either side of
        # realtime window
        datestamp = convert_SINGLE_datetime_to_unix(date=date)
        tt_time_from = int(datestamp + (self.time_from * 60 * 60) - 1800)
        tt_time_to = int(datestamp + (self.time_to * 60 * 60) + 1800)
        if self.partial_timetable:
            df = df.filter(
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

    def build_realtime(
        self, region: str, date: str
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Process realtime for given region and day.

        Apply unique identifier to each service stop and
        concat realtime data.

        Parameters
        ----------
        region : str
            Region name of real time data to ingest and process.
        date : str
            Date in string format %Y%m%d.

        Returns
        -------
        df : polars.DataFrame
            Dataframe of lbelled real time data for region concatenated.
        unlabelled : polars.DataFrame
            Dataframe of unlabelled real time data. (Currently not used)

        """
        if region is None:
            region = self.region

        if date is None:
            date = self.date

        df = self.load_raw_realtime_data(region, date)
        df, unlabelled = self.split_realtime_data(df)
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
        df = df.unique("UID", keep="last")

        return df, unlabelled

    def punctuality_by_stop(
        self, realtime_df: pl.DataFrame, timetable_df: pl.DataFrame
    ) -> pl.DataFrame:
        """Apply punctuality flag to RT/TT combined schedule.

        Parameters
        ----------
        realtime_df : polars.DataFrame
            Dataframe containing real time bus data.
        timetable_df : polars.DataFrame
            Dataframe containing timetable bus data.

        Returns
        -------
        df : polars.DataFrame
            Dataframe containing punctual flag of each bus stop ping.

        """
        df = (
            realtime_df[["UID", "time_transpond", "bus_id"]]
            .join(timetable_df, on="UID", how="inner")
            .unique()
        )

        df = df.with_columns(
            (pl.col("unix_arrival_time") - pl.col("time_transpond")).alias(
                "relative_punctuality"
            )
        )
        df = df.with_columns(
            pl.when(
                (pl.col("relative_punctuality") > -300)
                & (pl.col("relative_punctuality") < 60)
            )
            .then(1)
            .otherwise(0)
            .alias("punctual")
        )
        df = df.group_by(["stop_id", "stop_lat", "stop_lon"]).agg(
            [
                pl.count("punctual").alias("service_stops"),
                pl.mean("punctual").alias("punctuality_rate"),
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

    # load config toml
    config = toml.load("src/bus_metrics/aggregation/config.toml")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--date",
        required=False,
        default=datetime.today().strftime("%Y%m%d"),
        type=str,
    )
    args = vars(parser.parse_args())

    date = args["date"]

    # load stops file
    stops = build_stops()

    # Instantiate class
    builder = Schedule_Builder(
        **config,
    )

    # could this be in a loop for a larger england downoad?
    tt = builder.build_timetable(stops=stops, region=builder.region, date=date)

    # Method returns unlabelled, but not ingested
    rt, unlabelled = builder.build_realtime(region=builder.region, date=date)

    final_df_script = builder.punctuality_by_stop(
        realtime_df=rt, timetable_df=tt
    )

    if builder.output_unlabelled_bulk:
        logger.info("Exporting unlablled data to file.")
        unlabelled = unlabelled.to_pandas()
        unlabelled.to_csv(
            f"outputs/unlabelled_{builder.region}_{date}.csv",
            index=False,
        )

    logger.info("Writing punctuality to file")
    pandas_df = (
        final_df_script.to_pandas()
        .sort_values("stop_id")
        .reset_index(drop=True)
    )
    pandas_df.to_csv(
        f"data/stop_level_punctuality/punctuality_by_stop_{builder.region}_{date}.csv",  # noqa: E501
        index=False,
    )

    logger.info("\n")
    logger.info("-------------------------------")
    logger.info("Schedule Builder jobs completed")
    logger.info("-------------------------------")
