"""Class of tools required to reaggregate bus metrics by geographies."""
import pandas as pd
import os
import toml

from src.bus_metrics.setup.ingest_static_data import StaticDataIngest

config = toml.load("src/bus_metrics/setup/ingest.toml")


class AggregationTool:
    """Aggregate bus metrics by geographies."""

    def __init__(
        self,
        stop_level_punctuality="data/stop_level_punctuality/synth_punc.csv",
        config=toml.load("src/bus_metrics/setup/ingest.toml"),
        geography="lsoa",
    ) -> None:

        self.stop_level_punctuality = stop_level_punctuality
        self.geography = geography
        self.code = config["boundaries"][self.geography]["code"]
        self.name = config["boundaries"][self.geography]["name"]

    def _check_boundaries_local(self):
        if not os.path.exists(
            f"data/resources/{self.geography}_boundaries.geojson"
        ):
            url = config["boundaries"][self.geography]
            StaticDataIngest.ingest_data_from_geoportal(url)

        return None

    def _merge_geographies_with_stop_punctuality(self):
        stops = pd.read_csv(self.stop_level_punctuality, index_col=0)
        lookup = pd.read_csv(
            "data/resources/geography_lookup_table.csv", index_col=0
        )
        df = pd.merge(stops, lookup, on="stop_id", how="left")
        return df

    def _reaggregate_punctuality(self, labelled: pd.DataFrame = None):
        labelled["punctual_service_stops"] = (
            labelled["service_stops"] * labelled["punctuality_rate"]
        ).astype(int)

        agg_df = labelled.groupby([self.code, self.name]).agg(
            {"service_stops": "sum", "punctual_service_stops": "sum"}
        )
        agg_df["punctuality_rate"] = (
            agg_df["punctual_service_stops"] / agg_df["service_stops"]
        )
        # agg_df = labelled.groupby([self.code, self.name]).pipe(
        #     lambda x: x["punctual_service_stops"].sum()
        #     / x["service_stops"].sum()
        # )
        agg_df = agg_df.reset_index()
        # agg_df.columns = [self.code, self.name, "punctuality_rate"]
        return agg_df

    def punctuality_by_geography(self):
        """Collect punctuality data and reaggregate."""
        self._check_boundaries_local()
        df = self._merge_geographies_with_stop_punctuality()
        df = self._reaggregate_punctuality(df)
        df.to_csv(f"outputs/punctuality/{self.geography}.csv")
        return df
