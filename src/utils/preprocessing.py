import zipfile
import os
import glob
import pandas as pd
import polars as pl
import geopandas as gpd
from convertbng.util import convert_lonlat


def deduplicate(df):
    """Deduplicates realtime BODS data in respect of
    available variables to leave only unique pings
    in respect of specified variables at their latest
    transmission time"""
    df = df.drop_duplicates(
        [
            "bus_id",
            "time_transpond",
            "latitude",
            "longitude",
            "bearing",
            "journey_date",
        ],
        keep="last",
    )
    return df


def zip_files(to_dir, zip_name):
    """Zips required GTFS constituent files into folder"""

    # List of .txt files to add to the .zip file
    txt_files = [
        "agency.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "feed_info.txt",
        "routes.txt",
        "shapes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]

    # Zips file's name
    zip_file = f"{to_dir}/{zip_name}"

    # Create a new .zip file
    with zipfile.ZipFile(f"{zip_file}", "w", zipfile.ZIP_DEFLATED) as archive:
        for txt_file in txt_files:
            # Add each .txt file to the zip
            archive.write(
                f"{to_dir}/{txt_file}", arcname=txt_file
            )  # noqa: E501

    return None


def unzip_GTFS(
    txt_path: str,
    zip_path: str,
    file_name_pattern: str = "timetable.zip",
    logger=None,  # noqa:E501
):
    """Checks for all .txt files in folder.
    If any are missing, unzips the timetable.zip

    Params:

    txt_path (str): Path to where .txt files located/to unzip to.

    zip_path (str): Path to dir where .zip file is located.

    file_name_pattern (str): OPTIONAL suffix of file to unzip,
            defaults 'timetable.zip'.

    logger: OPTIONAL logger object used to collect info of process.

    Returns:

    Contents of zip folder."""

    txt_files = [
        "agency.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "feed_info.txt",
        "routes.txt",
        "shapes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]

    missing_files = [
        file
        for file in txt_files
        if not os.path.exists(os.path.join(txt_path, file))  # noqa: E501
    ]

    if missing_files:
        if logger:
            logger.info(
                f"Missing files: {missing_files}. Searching zip GTFS."  # noqa:E501
            )

        matching_zip = glob.glob(
            os.path.join(zip_path, f"*{file_name_pattern}")
        )  # noqa:E501

        # Checks there is a matching zip file
        if not matching_zip:
            if logger:
                logger.info("No GTFS zip in directory.")
            raise ValueError("No {file_name_pattern} in {zip_path}")

        # check there is not more than 1 zip file
        if len(matching_zip) > 1:
            if logger:
                logger.info(f"More than 1 {file_name_pattern} in {zip_path}")
            raise ValueError("More than 1 file_name_pattern in zip_path")

        # Unzips contents of zip to txt path
        zip_full_path = matching_zip[0]
        with zipfile.ZipFile(zip_full_path, "r") as zip_ref:
            # Extract to path
            zip_ref.extractall(txt_path)


def convert_SINGLE_datetime_to_unix(date=None):
    timestamp = (
        pd.to_datetime(
            str(date) + " " + "00:00:00", format="%Y%m%d %H:%M:%S"
        )  # noqa:E501
        .tz_localize("Europe/London")
        .tz_convert("UTC")
    )
    timestamp = (
        timestamp - pd.Timestamp("1970-01-01", tz="UTC")
    ) // pd.Timedelta(  # noqa:E501
        "1s"
    )  # noqa: E501

    return timestamp


def convert_string_time_to_unix(df=None, time_column=None):
    """Add additional column: unix timestamps to date string"""

    # TODO: refactor into one
    df = df.with_columns(
        pl.format("{} {}", "timetable_date", time_column).alias(
            f"unix_{time_column}"
        )
    )
    df = df.with_columns(
        pl.col(f"unix_{time_column}").str.to_datetime("%Y%m%d %H:%M:%S")
    )
    df = df.with_columns(
        pl.col(f"unix_{time_column}")
        #        .cast(pl.Datetime)
        .dt.replace_time_zone("Europe/London")
    )
    df = df.with_columns(
        pl.col(f"unix_{time_column}").dt.convert_time_zone("UTC")
    )
    df = df.with_columns(pl.col(f"unix_{time_column}").dt.epoch(time_unit="s"))

    return df


def convert_unix_to_time_string(df=None, unix_column=None):
    """Converts column of unix timestamps to time string
    e.g. 1698998880 -> 08:08:00"""
    df = df.with_columns(
        pl.from_epoch(pl.col(unix_column), time_unit="s").alias(
            f"dt_{unix_column}"
        )
    )
    df = df.with_columns(
        pl.col(f"dt_{unix_column}")
        .cast(pl.Datetime)
        .dt.replace_time_zone("UTC")
    )
    df = df.with_columns(
        pl.col(f"dt_{unix_column}").dt.convert_time_zone("Europe/London")
    )
    df = df.with_columns(pl.col(f"dt_{unix_column}").cast(pl.Time))

    return df


def build_daily_stops_file(date):
    dir = "data/daily/timetable"
    regions = [
        "EastAnglia",
        "EastMidlands",
        "NorthEast",
        "NorthWest",
        "SouthEast",
        "SouthWest",
        "WestMidlands",
        "Yorkshire",
    ]

    df = pd.DataFrame()

    for region in regions:
        reg_stops = pd.read_csv(f"{dir}/{region}/{date}/stops.txt")
        df = pd.concat([df, reg_stops])

    df = df.drop_duplicates(subset="stop_id")
    return df


def build_stops(output: str = "polars") -> pl.DataFrame | pd.DataFrame:
    # import NapTAN data

    stops = pl.read_csv(
        "data/daily/gb_stops.csv",
        ignore_errors=True,
        dtypes={"stop_id": pl.Utf8},  # noqa: E501
    )
    stops = stops.filter(pl.col("Status") == "active")
    eastings = stops["Easting"].to_list()
    northings = stops["Northing"].to_list()
    lon, lat = convert_lonlat(eastings, northings)

    # TODO: fill nan rather than overwrite whole columns
    stops.with_columns(pl.Series(name="Latitude", values=lat))
    stops.with_columns(pl.Series(name="Longitude", values=lon))
    stops = stops[["ATCOCode", "Latitude", "Longitude"]]
    stops.columns = ["stop_id", "stop_lat", "stop_lon"]

    if output == "pandas":
        stops = stops.to_pandas()

    return stops


def apply_geography_label(
    df: pd.DataFrame,
    bounds: gpd.GeoDataFrame,
    type: str = "timetable",
    output: str = "polars",
) -> pl.DataFrame | pd.DataFrame:
    if type == "timetable":
        lat = "stop_lat"
        lon = "stop_lon"
    else:
        lat = "latitude"
        lon = "longitude"

    df["geometry"] = gpd.points_from_xy(df[lon], df[lat])  # noqa: E501
    df = gpd.GeoDataFrame(df)
    df = df.set_crs("4326")

    df_labelled = df.sjoin(bounds, how="left", predicate="within")
    # remove geometry to avoid polars conflict (pyarrow numpy issue)
    df_labelled = df_labelled.drop(columns="geometry")

    if output == "pandas":
        df_labelled = pd.DataFrame(df_labelled)
    else:
        df_labelled = pl.from_pandas(pd.DataFrame(df_labelled))

    return df_labelled
