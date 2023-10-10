import os
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import shutil


class GTFS_Builder:

    """
    Takes realtime and timetable data for a specified day, deduplicating
    and replacing timetabled times with actual realtime times. An updated
    GTFS folder is returned.
    """

    def __init__(self):
        self.today = "20230829"
        self.weekday = "tuesday"
        self.dir = "data/realtime/north_east_scoping_20230829"
        self.timetable_exceptions = True
        self.route_stop_threshold = 5

    def load_raw_realtime_data(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Collects and concatenates all realtime data for specified day

        Returns:
            labelled_real (pandas df): rows with trip_id & route_id
            unlabelled_real (pandas df): row with trip_id & route_id MISSING
        """

        # collate all realtime ingests to single dataframe
        tables = os.path.join(self.dir, "realtime*.csv")  # noqa: E501
        tables = glob.glob(tables)
        real = pd.concat(map(pd.read_csv, tables), ignore_index=True)
        real = real.iloc[:, 1:]
        real = real.sort_values("time_ingest")

        # deduplicated, labelled realtime data
        labelled_real = real.dropna(subset=["trip_id", "route_id"])
        labelled_real.drop_duplicates(
            subset=["bus_id", "time_transpond"], keep="first", inplace=True
        )

        labelled_real.drop_duplicates(
            subset=["trip_id", "current_stop"], keep="last", inplace=True
        )

        # deduplicated, unlabelled realtime data
        unlabelled_real = real[
            (real["trip_id"].isna()) & (real["route_id"].isna())
        ]  # noqa: E501
        unlabelled_real.drop_duplicates(
            subset=["bus_id", "time_transpond"], keep="first", inplace=True
        )

        return labelled_real, unlabelled_real

    def reclaim_unlabelled_by_busid(self, labelled_real, unlabelled_real):
        """
        ### NOT FUNCTIONAL YET ###
        Assigns trip_id & route_id by common bus_id.

        Returns:
            labelled_real (pandas df): rows with trip_id & route_id
            unlabelled_real (pandas df): row with trip_id & route_id MISSING
        """
        trips = labelled_real.set_index("bus_id")["trip_id"].to_dict()
        unlabelled_real["trip_id"] = unlabelled_real["trip_id"].fillna(
            unlabelled_real["bus_id"].map(trips)
        )
        routes = labelled_real.set_index("bus_id")["route_id"].to_dict()
        unlabelled_real["route_id"] = unlabelled_real["route_id"].fillna(
            unlabelled_real["bus_id"].map(routes)
        )

        reclaimed_real = unlabelled_real.dropna()
        unlabelled_real = unlabelled_real[unlabelled_real["trip_id"].isna()]

        labelled_real = pd.concat([labelled_real, reclaimed_real])

        return labelled_real, unlabelled_real

    def load_raw_timetable_data(self) -> pd.DataFrame:
        """
        Collects and concatenates all timetable data for specified
        day, returning all individual service stops (every 'bus
        at stop' activity) for the day.

        Returns:
            service_stops (pandas_df): all service stops
        """

        # id services affected by exceptions today only
        calendar_dates = pd.read_csv(
            f"{self.dir}/gtfs/calendar_dates.txt",
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
            f"{self.dir}/gtfs/trips.txt",
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
            f"{self.dir}/gtfs/stops.txt",
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
            f"{self.dir}/gtfs/stop_times.txt",
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
            f"{self.dir}/gtfs/routes.txt",
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
            f"{self.dir}/gtfs/calendar.txt",
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

    def create_route_linestrings(self, df):
        """
        ### NOT FUNCTIONAL AS YET
        Constructs linestring for each physical route, following stop sequence
        """

        # convert to GeoDataFrame with geometry column for coordinates
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df["stop_lat"], df["stop_lon"])
        )

        # ensure only unique points in any route
        routes = gdf.drop_duplicates(["route_id", "geometry"])

        # filter rows attributed to routes with at least
        # minimum number of unique points
        route_counts = routes["route_id"].value_counts()
        mask = routes["route_id"].isin(
            route_counts[route_counts > self.route_stop_threshold].index
        )
        routes = routes[mask]

        routes = (
            routes.groupby("route_id")
            .apply(lambda x: LineString(x.geometry))
            .reset_index()
        )

        routes.columns = ["route_id", "geometry"]

        routes = gpd.GeoDataFrame(routes)
        routes.to_file(f"{self.dir}/route_linestrings.geojson")

        return routes

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
        if not os.path.exists(f"{self.dir}/realtime_gtfs"):
            os.mkdir(f"{self.dir}/realtime_gtfs")

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
        ].to_csv(f"{self.dir}/realtime_gtfs/trips.txt", index=False)

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
        ].to_csv(f"{self.dir}/realtime_gtfs/stop_times.txt", index=False)

        # reverse-engineered stop_times.txt
        gtfs_temp[
            [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_type",
            ]
        ].to_csv(f"{self.dir}/realtime_gtfs/routes.txt", index=False)

        cal_dates = pd.DataFrame(
            columns=["service_id", "date", "exception_type"]
        )  # noqa: E501
        cal_dates.to_csv(
            f"{self.dir}/realtime_gtfs/calendar_dates.txt", index=False
        )  # noqa: E501

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
        calendar.to_csv(f"{self.dir}/realtime_gtfs/calendar.txt", index=False)

        # copy other admin files across - no changes required
        shutil.copy(
            f"{self.dir}/gtfs/agency.txt",
            f"{self.dir}/realtime_gtfs/agency.txt",  # noqa: E501
        )
        shutil.copy(
            f"{self.dir}/gtfs/feed_info.txt",
            f"{self.dir}/realtime_gtfs/feed_info.txt",  # noqa: E501
        )
        shutil.copy(
            f"{self.dir}/gtfs/shapes.txt",
            f"{self.dir}/realtime_gtfs/shapes.txt",  # noqa: E501
        )
        shutil.copy(
            f"{self.dir}/gtfs/stops.txt", f"{self.dir}/realtime_gtfs/stops.txt"
        )  # noqa: E501

        return None
