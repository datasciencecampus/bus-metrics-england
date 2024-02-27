"""Initial pipeline to obtain project resources/data."""
from src.bus_metrics.setup.ingest_static_data import StaticDataIngest
from src.bus_metrics.setup.ingest_realtime_data import RealtimeDataIngest
from src.bus_metrics.setup.build_lookup import create

# from src.bus_metrics.aggregation.build_schedules import Schedule_Builder
from datetime import datetime
import logging
import os
import toml
import time

sTool = StaticDataIngest()
rTool = RealtimeDataIngest()
ingest_toml = toml.load("src/bus_metrics/setup/ingest.toml")
scriptStartTime = datetime.now()
scriptStartTimeUnix = time.mktime(scriptStartTime.timetuple())


def data_folder(logger: logging.Logger) -> None:
    """Set up initial filesystem structure.

    Parameters
    ----------
    logger: logging.Logger
        Logger instance

    """
    if not os.path.exists("data/"):
        os.mkdir("data/")
        logger.info("Creating data folders")

    data_folders = ["realtime", "resources", "timetable"]
    for folder in data_folders:
        if not os.path.exists(f"data/{folder}"):
            os.mkdir(f"data/{folder}")

    return None


if __name__ == "__main__":  # noqa: C901
    session_name = f"ingest_{format(datetime.now(), '%Y_%m_%d_%H:%M')}"
    logger = logging.getLogger(__name__)
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        filename=f"log/{session_name}.log",
        filemode="a",
    )

    data_folder(logger=logger)

    try:
        logger.info("Importing NAPTAN stops data")
        sTool.import_stops_from_naptan()
    except Exception as e:
        logger.warning(f"Bypassed: {e}")
        pass

    try:
        logger.info("Importing boundaries data")
        sTool.ingest_data_from_geoportal()
    except Exception as e:
        logger.warning(f"Bypassed: {e}")
        pass

    try:
        logger.info("Importing bus timetable data")
        sTool.ingest_bus_timetable()
    except Exception as e:
        logger.warning(f"Bypassed: {e}")
        pass

    if ingest_toml["download_realtime_sample"]:

        scriptStartTime = datetime.now()
        scriptStartTimeUnix = time.mktime(scriptStartTime.timetuple())

        # TODO: more articulate ways of triggering every 10 seconds
        while (
            time.mktime(datetime.now().timetuple()) < scriptStartTimeUnix + 60
        ):
            try:
                logger.info("Importing example realtime data")
                rTool.api_call()
                rTool.parse_realtime()
                time.sleep(10)
            except Exception as e:
                logger.warning(f"Broken: {e}")
                logger.warning("##Recommend checking your API key first")
                break
    else:
        logger.warning("##Realtime download bypassed##")
        logger.warning("##Amend toml to download sample##")
        logger.warning("##Use shell script to download heavy##")

    try:
        logger.info("Building stops-geography lookup table")
        # TODO: naming/access of function to be improved
        create()
    except Exception as e:
        logger.warning(f"Build error: {e}")
        pass

    # build_schedules.py executed separately for now
    # try:
    #     schedule_builder = Schedule_Builder()
    #     logger.info("Preparing punctuality data by stop")
    #     # TODO: naming/access of method to be improved
    #     schedule_builder.run()
    # except Exception as e:
    #     logger.warning(f"Build error: {e}")
    #     pass

    logger.info("-----------------------------")
    logger.info("-------SETUP COMPLETED-------")
    logger.info("-----------------------------")
