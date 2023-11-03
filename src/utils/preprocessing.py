import zipfile


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
            txt_loc = f"{to_dir}/{txt_file}"
            # Add each .txt file to the zip
            archive.write(f"{to_dir}/{txt_file}", arcname=txt_loc)  # noqa: E501

    return None
