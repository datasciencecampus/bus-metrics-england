"""Class to ingest project resources."""

import geopandas as gpd
import naptan
import os
import pandas as pd
import requests
import toml
from datetime import datetime


class StaticDataIngest:
    """Ingest project resource data.

    Parameters
    ----------
    naptan_filename: str
        Full filepath to store NAPTAN data locally

    Attributes
    ----------
    config: dict
        Dictionary of imported toml ingest variables
    geography: str
        Geography by which data is to be aggregated
    region: str
        Region to be analysed
    timetable_url_prefix: str
        Consistent URL stem
    geoportal_query_params: dict
        Dictionary of parameters required in ONS GeoPortal
        API call
    zip_fp_root: str
        Root filepath to zip storage location

    Methods
    -------
    import_stops_from_naptan
        Ingest and store NAPTAN stops data
    ingest_data_from_geoportal
        Ingest and store required boundaries data
    ingest_bus_timetable
        Ingest and store current timetable for selected region

    """

    def __init__(
        self,
        naptan_filename: str = "data/resources/gb_stops.csv",
    ):
        self.config = toml.load("src/bus_metrics/setup/ingest.toml")
        self.naptan_filename = naptan_filename
        self.geography = self.config["geography"]
        self.region = self.config["region_to_analyse"]
        self.timetable_url_prefix = self.config["timetable_url_prefix"]
        self.geoportal_query_params = self.config["geoportal_query_params"]
        self.boundaries = self.config["boundaries"]
        self.zip_fp_root: str = "data/timetable"

    def import_stops_from_naptan(self, filename: str = None) -> None:
        """Import and store NAPTAN stops data.

        Parameters
        ----------
        filename: str
            Filepath to Local storage location

        Raises
        ------
        FileExistsError
            When NAPTAN data already exists at given filepath

        """
        # TODO: consider if these need to be time-stamped as they are
        # updated by NAPTAN daily
        if filename is None:
            filename = self.naptan_filename
        if not os.path.exists(filename):
            stops = naptan.get_all_stops()
            stops.to_csv(filename)
        else:
            raise FileExistsError(
                "The file you are downloading to already exists (naptan)"
            )

        return None

    def _connect_to_endpoint(self, url: str) -> dict | Exception:
        """Diagnose successful connection to site.

        Parameters
        ----------
        url: str
            API endpoint

        Returns
        -------
        response: dict
            Full response content from API call

        Raises
        ------
        RequestException
            Indicates bad response from API call

        """
        if url is None:
            url = self.boundaries[self.geography]["url"]
        response = requests.get(url, params=self.geoportal_query_params)

        if response.ok:
            return response
        else:
            # cases where a traditional bad response may be returned
            raise requests.RequestException(
                f"HTTP Code: {response.status_code}, Status: {response.reason}"
            )

    def _extract_geodata(self, content: dict) -> gpd.GeoDataFrame:
        """Extract geodataframe object from HTML response content element.

        Parameters
        ----------
        content: dict
            Content element of HTML response

        Returns
        -------
        gdf: gpd.GeoDataFrame
            Raw GeoDataFrame of full content returned from API call

        """
        gdf = gpd.GeoDataFrame.from_features(
            content["features"], crs=content["crs"]["properties"]["name"]
        )
        return gdf

    def ingest_data_from_geoportal(
        self, url: str = None, filename: str = None
    ) -> None:
        """Ingest data from API endpoint of ONS GeoPortal.

        Parameters
        ----------
        url: str
            API endpoint
        filename: str
            Filepath to local storage location

        Raises
        ------
        FileExistsError
            When boundaries data already exists at given filepath

        """
        if url is None:
            url = self.boundaries[self.geography]["url"]
        if filename is None:
            filename = self.boundaries[self.geography]["filename"]

        if not os.path.exists(filename):
            query_params = self.geoportal_query_params
            response = self._connect_to_endpoint(url)
            content = response.json()
            gdf = self._extract_geodata(content)

            more_pages = content["properties"]["exceededTransferLimit"]
            offset = len(gdf)  # rows in initial cut

            while more_pages:
                query_params["resultOffset"] += offset
                response = self._connect_to_endpoint(url)
                content = response.json()
                add_gdf = self._extract_geodata(content)
                gdf = pd.concat([gdf, add_gdf])
                try:
                    more_pages = content["properties"]["exceededTransferLimit"]
                except KeyError:
                    # exceededTransferLimit field no longer present
                    more_pages = False

            gdf = gdf.reset_index(drop=True)
            gdf.to_file(filename, driver="GeoJSON")

        else:
            raise FileExistsError(
                "The file you are downloading to already exists (bounds)"
            )

        return None

    def ingest_bus_timetable(self, region: str = None) -> None:
        """Ingest bus timetable for a single region.

        Parameters
        ----------
        region: str
            Region data to be ingested

        Raises
        ------
        FileExistsError
            When timetable data already exists at given filepath

        """
        date = datetime.now().date().strftime("%Y%m%d")
        if region is None:
            url = f"{self.timetable_url_prefix}/{self.region.lower()}"
            filename = f"{self.zip_fp_root}/{self.region.lower()}_{date}.zip"
        else:
            url = f"{self.timetable_url_prefix}/{region}"
            filename = f"{self.zip_fp_root}/{region}_{date}.zip"

        if not os.path.exists(filename):
            r = self._connect_to_endpoint(url)

            with open(filename, "wb") as f:
                f.write(r.content)

        else:
            raise FileExistsError(
                "The file you are downloading to already exists (timetable)"
            )

        return None
