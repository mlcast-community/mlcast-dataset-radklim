# mlcast-dataset-radklim

This repository contains code to convert the [RADKLIM dataset](https://opendata.dwd.de/climate_environment/CDC/help/landing_pages/doi_landingpage_RADKLIM_RW_V2017.002-en.html) to zarr.

Eventually this repo with also contain an intake catalog with a reference to the data uploaded to the European Weather Cloud

## Structure

The processing is implemented in `luigi` Tasks, in `mlcast_dataset_radklim/source.py` and `mlcast_dataset_radklim/zarr.py`.

The working directory for data is `data/` with following structure:

```
data
├── dst
│   ├── jsons
│   │   └── RW2017.002_2020_netcdf.json
│   └── zarr
│       └── RW2017.002_2020_netcdf.zarr
└── src
    ├── compressed
    │   ├── RW2017.002_2020_netcdf.tar.gz
    │   └── RW2017.002_2021_netcdf.tar.gz
    └── uncompressed
        ├── RW2017.002_2020_netcdf
        │   └── 2020
        │       ├── RW_2017.002_202001.nc
        │       ├── RW_2017.002_202002.nc
        │       ├── RW_2017.002_202003.nc
        │       ├── RW_2017.002_202004.nc
        │       ├── RW_2017.002_202005.nc
        │       ├── RW_2017.002_202006.nc
        │       ├── RW_2017.002_202007.nc
        │       ├── RW_2017.002_202008.nc
        │       ├── RW_2017.002_202009.nc
        │       ├── RW_2017.002_202010.nc
        │       ├── RW_2017.002_202011.nc
        │       └── RW_2017.002_202012.nc
        └── RW2017.002_2021_netcdf
            └── 2021
                ├── RW_2017.002_202101.nc
                ├── RW_2017.002_202102.nc
                ├── RW_2017.002_202103.nc
                ├── RW_2017.002_202104.nc
                ├── RW_2017.002_202105.nc
                ├── RW_2017.002_202106.nc
                ├── RW_2017.002_202107.nc
                ├── RW_2017.002_202108.nc
                ├── RW_2017.002_202109.nc
                ├── RW_2017.002_202110.nc
                ├── RW_2017.002_202111.nc
                └── RW_2017.002_202112.nc
```