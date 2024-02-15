"""File containing functions to use in build_schedules."""

import zipfile
import os
import glob
import pandas as pd
import polars as pl
from convertbng.util import convert_lonlat
import logging


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate dataframe.

    Deduplicates realtime BODS data in respect of
    available variables to leave only unique pings
    in respect of specified variables at their latest
    transmission time.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe to remove duplicates from.

    Returns
    -------
    df : pandas.DataFrame
        Processed dataframe without duplicates.

    """
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


def zip_files(to_dir: str, zip_name: str):
    """Zips required GTFS constituent files into folder.

    Parameters
    ----------
    to_dir : str
        String of path to directory containing GTFS files to zip.
    zip_name : str
        File name to save zipped files as.

    """
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
    logger: logging.Logger = None,  # noqa:E501
):
    """Check for all .txt files in folder.

    If any are missing, unzips the timetable.zip.

    Parameters
    ----------
    txt_path : str
        Path to where .txt files located/to unzip to.
    zip_path: str
        Path to dir where .zip file is located.
    file_name_pattern : str, optional
        Suffix of file to unzip defaults 'timetable.zip'.
    logger : logger.Logger, optional
        Logger object used to collect info of process.

    """
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
            raise ValueError(f"No {file_name_pattern} in {zip_path}")

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


def convert_SINGLE_datetime_to_unix(date: str = None) -> pd.Timestamp:
    """Convert a single date to UNIX format.

    Parameters
    ----------
    date : str
        Date in which to convert to UNIX.

    Returns
    -------
    timestamp : pd.Timestamp
        Timestamp at mightnight 00:00:00 of the provided date.

    """
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


def convert_string_time_to_unix(
    df: pl.DataFrame = None, time_column: str = None
) -> pl.DataFrame:
    """Add additional column: unix timestamps to date string.

    Parameters
    ----------
    df : polars.DataFrame
        Dataframe containing timetable data.
    time_column : str
        Name of columns containing string fomat time.

    Returns
    -------
    df : polars.DataFrame
        Dataframe containing new column with UNIX format time.

    """
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


def convert_unix_to_time_string(
    df: pl.DataFrame = None, unix_column: str = None
) -> pl.DataFrame:
    """Convert column of unix timestamps to time string.

    Convert unix column containing a timetstamp to a string format
    e.g. 1698998880 -> 08:08:00.

    Parameters
    ----------
    df : polars.DataFrame
        Dataframe containing UNIX time column.
    unix_column : str
        Name of column which contains UNIX values.

    Returns
    -------
    df : polars.DataFrame
        Dataframe with new column in string format.

    """
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


def build_stops(output: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """Read in gb_stops file and outouts as DataFrame.

    Parameters
    ----------
    output : str
        Output type, polars or pandas DataFrame. (Defaults "polars").

    Returns
    -------
    stops : polars or pandas DataFrame
        Dataframe containing stop data.

    """
    # import NapTAN data
    stops = pl.read_csv(
        "data/resources/gb_stops.csv",
        ignore_errors=True,
        dtypes={"stop_id": pl.Utf8},  # noqa: E501
    )

    eastings = stops["Easting"].to_list()
    northings = stops["Northing"].to_list()
    lon, lat = convert_lonlat(eastings, northings)

    stops = stops.with_columns(pl.Series(name="LatitudeNew", values=lat))
    stops = stops.with_columns(pl.Series(name="LongitudeNew", values=lon))
    stops = stops.with_columns(
        [
            (pl.col("Latitude").fill_null(pl.col("LatitudeNew"))),
            (pl.col("Longitude").fill_null(pl.col("LongitudeNew"))),
        ]
    )

    stops = stops[["ATCOCode", "Latitude", "Longitude"]]
    stops.columns = ["stop_id", "stop_lat", "stop_lon"]

    if output == "pandas":
        stops = stops.to_pandas()

    return stops


def polars_robust_load_csv(filepath: str, dtypes: dict = None) -> pl.DataFrame:
    """Load csv using polars with resilient settings.

    Prameters
    ---------
    filepath : str
        Filepath to csv to load.
    dtypes : dict, optional
        Dictionary of columns and dtypes to load as.

    Returns
    -------
    df : polars.DataFrame
        Dataframe of csv from filepath.

    """
    if dict:
        df = pl.read_csv(
            filepath,
            ignore_errors=True,
            infer_schema_length=None,
            dtypes=dtypes,
        )
    else:
        df = pl.read_csv(
            filepath, ignore_errors=True, infer_schema_length=None
        )
    return df
