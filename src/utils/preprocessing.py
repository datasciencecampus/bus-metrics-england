import zipfile
import os
import glob


def deduplicate(df):
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
            archive.write(f"{to_dir}/{txt_file}", arcname=txt_file)  # noqa: E501

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
            logger.info(f"More than 1 {file_name_pattern} in {zip_path}")
            raise ValueError("More than 1 file_name_pattern in zip_path")

        # Unzips contents of zip to txt path
        zip_full_path = matching_zip[0]
        with zipfile.ZipFile(zip_full_path, "r") as zip_ref:
            # Extract to path
            zip_ref.extractall(txt_path)
