"""Runs pipeline to reaggregate bus metrics by geography."""
import argparse
from src.bus_metrics.aggregation.punctuality_rate import AggregationTool


def main():
    """Reaggregate stop-level punctuality by selected geography.

    Returns
    -------
    df: pandas.DataFrame
        DataFrame of number of service stops and
        punctuality rate by user-selected geography.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--geography", nargs="?", help="which geography")
    args = parser.parse_args()

    if not args.geography:
        geography = "lsoa"
    else:
        geography = args.geography

    aTool = AggregationTool(geography=geography)
    df = aTool.punctuality_by_geography()

    return df


if __name__ == "__main__":
    main()
