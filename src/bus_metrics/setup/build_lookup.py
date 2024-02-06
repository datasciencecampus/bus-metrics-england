"""Associate multiple geography labels to NAPTAN stops."""

# %%
import toml
import geopandas as gpd
import pandas as pd
from src.bus_metrics.setup.ingest_static_data import StaticDataIngest


def _build_lookup_tool(df, bounds, bounds_code, bounds_name):
    """Merge boundaries to stops."""
    df["geometry"] = gpd.points_from_xy(
        df["stop_lon"], df["stop_lat"]
    )  # noqa: E501
    df = gpd.GeoDataFrame(df)
    df = df.set_crs("4326")
    df_labelled = df.sjoin(bounds, how="left", predicate="within")

    cols = list(df.columns)
    cols.extend((bounds_code, bounds_name))
    df_labelled = df_labelled[cols]

    return df_labelled


def main():
    """Download and process boundaries data. Label stops."""
    installer = StaticDataIngest()
    config = toml.load("src/bus_metrics/setup/ingest.toml")
    boundaries = config["boundaries"]

    # TODO: address mixed dtypes pandas DtypeWarning
    stops = pd.read_csv("data/resources/gb_stops.csv", index_col=0)
    stops = stops[stops["Status"] == "active"]
    stops = stops[["ATCOCode", "Latitude", "Longitude"]]
    stops.columns = ["stop_id", "stop_lat", "stop_lon"]

    # TODO: consider tqdm progress bar for large file downloads
    for geog in boundaries:
        url = boundaries[geog]["url"]
        filename = boundaries[geog]["filename"]
        code = boundaries[geog]["code"]
        name = boundaries[geog]["name"]

        try:
            print(
                f"Downloading/Processing FULL RES(!) {geog} boundary data..."
            )
            installer.ingest_data_from_geoportal(url, filename)

        except FileExistsError:
            pass

        bounds = gpd.read_file(filename)
        stops = _build_lookup_tool(stops, bounds, code, name)
        stops = stops.drop(columns="geometry")

    print("Storing lookup file...")
    # note: currently retains all stops in NAPTAN data
    # irrespective of location across UK
    stops.to_csv("data/resources/geography_lookup_table.csv")

    return stops


if __name__ == "__main__":
    main()
