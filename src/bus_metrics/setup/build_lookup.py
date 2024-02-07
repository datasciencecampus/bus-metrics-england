"""Tool to generate lookup table for stops to various geography levels."""

import toml
import geopandas as gpd
import pandas as pd
from src.bus_metrics.setup.ingest_static_data import StaticDataIngest


def _build_lookup_tool(
    stops: pd.DataFrame,
    bounds: gpd.GeoDataFrame,
    bounds_code: str,
    bounds_name: str,
) -> pd.DataFrame:
    """Allocate geography labels to bus stops by means of a spatial join.

    Parameters
    ----------
    stops: pandas.DataFrame
        Dataframe of NAPTAN stops data.
    bounds: geopandas.GeoDataFrame
        Dataframe of geography boundaries data.
    bounds_code: str
        Geography code e.g. LSOA21CD
    bounds_name: str
        Geography name e.g. LSOA21NM

    Returns
    -------
    df: pandas.DataFrame
        Dataframe of stops-geography lookup table.

    """
    stops["geometry"] = gpd.points_from_xy(
        stops["stop_lon"], stops["stop_lat"]
    )
    stops = gpd.GeoDataFrame(stops)
    stops = stops.set_crs("4326")
    df = stops.sjoin(bounds, how="left", predicate="within")

    cols = list(stops.columns)
    cols.extend((bounds_code, bounds_name))
    df = df[cols]

    return df


def main() -> pd.DataFrame:
    """Download and process boundaries data. Label stops.

    Returns
    -------
    stops: pandas.DataFrame
        Dataframe of stops-geography lookup table.

    """
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
