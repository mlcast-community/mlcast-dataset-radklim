import pytest

from mlcast_dataset_radklim.source import create_url

EXAMPLES = [
    (
        dict(data_kind="hourly", year=2001),
        "https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/2001/RW2017.002_2001_netcdf.tar.gz",
    ),
    (
        dict(data_kind="hourly", year=2021),
        "https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/supplement/RW2017.002_2021_netcdf_supplement.tar.gz",
    ),
    (
        dict(data_kind="5_minutes", year=2001, month=1),
        "https://opendata.dwd.de/climate_environment/CDC/grids_germany/5_minutes/radolan/reproc/2017_002/netCDF/2001/YW2017.002_200101_asc.tar",
    ),
    (
        dict(data_kind="5_minutes", year=2021, month=1),
        "https://opendata.dwd.de/climate_environment/CDC/grids_germany/5_minutes/radolan/reproc/2017_002/netCDF/supplement/YW2017.002_2021_asc_supplement.tar.gz",
    ),
]


@pytest.mark.parametrize("input, expected_url", EXAMPLES)
def test_urls(input, expected_url):
    url = create_url(**input)
    assert url == expected_url
