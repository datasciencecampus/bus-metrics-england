# %%
import geopandas as gpd
import naptan
import os
import pandas as pd
import requests
import toml
from zipfile import ZipFile

class StaticDataIngest:

    def __init__(
            self,
            config:str = "src/setup/ingest.toml",
            naptan_filename:str = "data/resources/gb_stops.csv",
            geography:str = "lsoa",
            timetable_region:str = "Yorkshire",
    ):
        self.config = config
        self.naptan_filename = naptan_filename
        self.geography = geography
        self.timetable_region = timetable_region
        self.timetable_url_prefix = toml.load(config)["timetable_url_prefix"]
        self.geoportal_query_params = toml.load(config)["geoportal_query_params"]
        self.boundaries = toml.load(config)["boundaries"]

    def import_stops_from_naptan(self, filename:str = None
                                 ) -> None:
        """
        Imports all transport stops in Great Britain
        from NapTAN site and writes to local file
        Args:
            filename(str): local storage location
        """
        if filename is None:
            filename = self.naptan_filename
        if not os.path.exists(filename):
            stops = naptan.get_all_stops()
            stops.to_csv(filename)
        else:
            raise FileExistsError("The file you are downloading to already exists")

        return None


    def _connect_to_endpoint(self, url: str
                              ) -> dict|None:
        """
        Diagnoses successful connection to site
        Args:
            url(str): API endpoint
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

    def _extract_geodata(self, content:dict
                         ) -> gpd.GeoDataFrame:
        """
        Extracts geodataframe object from HTML response content element
        Args:
            content(dict): content element of HTML response
        """
        gdf = gpd.GeoDataFrame.from_features(
                content["features"],
                crs=content["crs"]["properties"]["name"]
            )
        return gdf

    def ingest_data_from_geoportal(self, url: str = None, filename: str = None
                                   ) -> None:
        """
        Ingests data from API endpoint of ONS GeoPortal
        Args:
            url(str): API endpoint
            filename(str): local storage location
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
                query_params[
                    "resultOffset"
                ] += offset
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
            raise FileExistsError("The file you are downloading to already exists")

        return None


    def ingest_bus_timetable(self, url:str = None, filename:str = None
                             ) -> None:
        # if either missing then pull defaults to avoid mismatches
        if (url is None)|(filename is None):
            url = f"{self.timetable_url_prefix}/{self.timetable_region.lower()}"
            filename = f"data/timetable/{self.timetable_region.lower()}.zip"

        if not os.path.exists(filename):
            r = self._connect_to_endpoint(url)

            with open(filename, "wb") as f:
                    f.write(r.content)

        else:
            raise FileExistsError("The file you are downloading to already exists")

        return None
