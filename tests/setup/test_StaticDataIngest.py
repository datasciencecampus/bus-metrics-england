import pytest
from src.setup.ingest_static_data import StaticDataIngest

tool = StaticDataIngest()

def test_ingest_bus_timetable_file_exists():
    with pytest.raises(FileExistsError) as excinfo:
        tool.ingest_bus_timetable(url="www", filename="tests/data/region.zip")
    assert str(excinfo.value) == "The file you are downloading to already exists"
