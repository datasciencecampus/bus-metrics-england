"""Class of tools required to reaggregate bus metrics by geographies."""
import pandas as pd
import toml


class AggregationTool:
    """Aggregate bus metrics by geographies.

    Parameters
    ----------
    stop_level_punctuality: str
        Full filepath to stop-level punctuality aggregated data
    config: dict
        Dictionary of imported toml ingest variables
    geography: str
        Geography by which data is to be aggregated

    Attributes
    ----------
    code: str
        Geography code in boundaries data e.g. LSOA21CD
    name: str
        Geography name in boundaries data e.g. LSOA21NM

    Methods
    -------
    punctuality_by_geography
        Combine stop-level punctuality data with boundaries
        and reaggregate

    """

    def __init__(
        self,
        stop_level_punctuality: str = "data/stop_level_punctuality/synth_punc.csv",  # noqa: E501
        config: dict = toml.load("src/bus_metrics/setup/ingest.toml"),
        geography: str = "lsoa",
    ) -> None:

        self.stop_level_punctuality = stop_level_punctuality
        self.geography = geography
        self.code = config["boundaries"][self.geography]["code"]
        self.name = config["boundaries"][self.geography]["name"]

    def _merge_geographies_with_stop_punctuality(
        self,
    ) -> pd.DataFrame | Exception:
        """Merge geography labels and stop-level punctuality.

        Returns
        -------
        df: pandas.DataFrame
            Dataframe of stop-level punctuality with all
            associated geography labels.

        Raises
        ------
        FileNotFoundError
            When either stops or geography lookup
            do not exist locally.

        """
        try:
            stops = pd.read_csv(self.stop_level_punctuality, index_col=0)
            lookup = pd.read_csv(
                "data/resources/geography_lookup_table.csv", index_col=0
            )
            df = pd.merge(stops, lookup, on="stop_id", how="left")
            return df

        except FileNotFoundError as e:
            raise e("Please re-run the build_lookup.py script.")

    def _reaggregate_punctuality(
        self, labelled: pd.DataFrame = None
    ) -> pd.DataFrame:
        """Re-aggregate stop-level punctuality by specified geography.

        Parameters
        ----------
        labelled: pandas.DataFrame
            Dataframe of stop-level punctuality with all
            available geography labels associated with each.

        Returns
        -------
        df: pandas.DataFrame
            Dataframe of number of service stops
            and punctuality rate aggregated by geography.

        """
        labelled["punctual_service_stops"] = (
            labelled["service_stops"] * labelled["punctuality_rate"]
        ).astype(int)
        df = labelled.groupby([self.code, self.name]).agg(
            {"service_stops": "sum", "punctual_service_stops": "sum"}
        )
        df["punctuality_rate"] = (
            df["punctual_service_stops"] / df["service_stops"]
        )
        df = df.reset_index()

        return df

    def punctuality_by_geography(self):
        """Collect punctuality data, reaggregate and store locally.

        Returns
        -------
        df: pandas.DataFrame
            Dataframe of number of service stops
            and punctuality rate aggregated by geography.

        """
        df = self._merge_geographies_with_stop_punctuality()
        df = self._reaggregate_punctuality(df)
        df.to_csv(f"outputs/punctuality/{self.geography}.csv")
        return df
