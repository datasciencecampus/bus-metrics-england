## Ingesting static data
- NAPTAN stops data
  (`StaticDataIngest.import_stops_from_naptan()`)
- Boundaries data
  (`StaticDataIngest.ingest_data_from_geoportal()`)
- Bus timetable data
  (`StaticDataIngest.ingest_bus_timetable()`)

Each of the above has default values stored in `src/setup/ingest.toml` and/or in the `__init__` of the `StaticDataIngest` class. However, they can also be implemented with custom values e.g.

```{python}
from src.setup.ingest_static_data import StaticDataIngest
tool = StaticDataIngest()
tool.ingest_bus_timetable(
    url="https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/north_east",
    filename="data/timetable/north_east.zip")
```
