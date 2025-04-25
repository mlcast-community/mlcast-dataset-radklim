import datetime
import json
import os
from pathlib import Path

import kerchunk.combine
import kerchunk.hdf
import luigi
import psutil
import xarray as xr
from dask.distributed import LocalCluster
from loguru import logger

from .__init__ import __version__
from .config import DATA_PATH
from .source import UntarYearTask


class KerchunkSingleYearTask(luigi.Task):
    """
    Task to build a JSON for Zarr using Kerchunk for a single year's netCDF files.

    Parameters
    ----------
    year : int
        The year for which to build the Zarr JSON.
    data_kind : str
        The kind of data to build the Zarr JSON for, either 'hourly' or '5_minutes'.
    """

    year = luigi.IntParameter()
    data_kind = luigi.Parameter()

    def requires(self):
        """
        Specifies the dependency on the UntarYearTask.
        """
        return UntarYearTask(year=self.year, data_kind=self.data_kind)

    def output(self):
        """
        Specifies the output target for the Kerchunk JSON file for a single year.
        """
        return luigi.LocalTarget(
            DATA_PATH / "dst" / self.data_kind / "jsons" / f"{self.year}_netcdf.json"
        )

    def run(self):
        """
        Uses Kerchunk to build a JSON for Zarr from the untarred netCDF files for a single year.
        """
        input_path = Path(self.input().path)
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        netcdf_files = list(input_path.glob("**/*.nc"))
        if len(netcdf_files) == 0:
            raise FileNotFoundError(
                f"No netCDF files found in the untarred directory for year {self.year}: {input_path}"
            )

        # for the 5-minute data where corrections were issued it appears they
        # include the old files in subdirs that have the suffix _old. We need
        # to ignore these
        files_to_exclude = [f for f in netcdf_files if f.parent.name.endswith("_old")]
        if len(files_to_exclude) > 0:
            logger.info(
                f"Excluding {len(files_to_exclude)} files with '_old' in the directory name"
            )
            netcdf_files = [f for f in netcdf_files if f not in files_to_exclude]

        logger.info(f"Found {len(netcdf_files)} netCDF files for year {self.year}")

        references = []
        for nc_file in netcdf_files:
            with open(nc_file, "rb") as f:
                h5chunks = kerchunk.hdf.SingleHdf5ToZarr(f, str(nc_file))
                # check that x, y, lat, lon and time are in the file
                for ref in [["lon/0.0", "lat/0.0", "time/0", "x/0", "y/0"]]:
                    if not any(r in str(ref) for r in ref):
                        raise ValueError(
                            f"Missing required reference in {nc_file}: {ref}"
                        )
                file_refs = h5chunks.translate()
                references.append(file_refs)

        mzz = kerchunk.combine.MultiZarrToZarr(
            references, concat_dims=["time"], identical_dims=["lat", "lon"]
        )
        combined_references = mzz.translate()

        # Write the combined references to the output JSON file
        with output_path.open("w") as f:
            json.dump(combined_references, f)

        # try opening the JSON file to check if it is valid
        xr.open_zarr(f"reference::{output_path}", consolidated=False)


class KerchunkMultiYearTask(luigi.Task):
    """
    Task to combine JSONs for multiple years using Kerchunk's MultiZarrToZarr.

    Parameters
    ----------
    start_year : int
        The start year for which to combine the Zarr JSONs.
    end_year : int
        The end year for which to combine the Zarr JSONs.
    data_kind : str
        The kind of data to combine the Zarr JSONs for, either 'hourly' or '5_minutes'.
    """

    start_year = luigi.IntParameter()
    end_year = luigi.IntParameter()
    data_kind = luigi.Parameter()

    def requires(self):
        """
        Specifies the dependency on KerchunkSingleYearTask for each year.
        """
        return {
            year: KerchunkSingleYearTask(year=year, data_kind=self.data_kind)
            for year in range(self.start_year, self.end_year + 1)
        }

    def output(self):
        """
        Specifies the output target for the combined Kerchunk JSON file.
        """
        return luigi.LocalTarget(
            DATA_PATH
            / "dst"
            / self.data_kind
            / "jsons"
            / f"{self.start_year}_{self.end_year}_netcdf.json"
        )

    def run(self):
        """
        Combines JSONs for multiple years using Kerchunk's MultiZarrToZarr.
        """
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        references = []
        for year, input_task in self.input().items():
            with open(input_task.path, "r") as f:
                year_references = json.load(f)
                references.extend([year_references])

        mzz = kerchunk.combine.MultiZarrToZarr(
            references, concat_dims=["time"], identical_dims=["lat", "lon"]
        )
        combined_references = mzz.translate()

        with output_path.open("w") as f:
            json.dump(combined_references, f)


def insert_bbox_into_wkt(
    crs_wkt: str, min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> str:
    """
    Inserts a WKT2-compliant BBOX group into the final position of a WKT string,
    just before the last closing bracket.

    Parameters:
    -----------
    crs_wkt : str
        The original WKT string.
    min_lon : float
        Western (minimum longitude) boundary.
    min_lat : float
        Southern (minimum latitude) boundary.
    max_lon : float
        Eastern (maximum longitude) boundary.
    max_lat : float
        Northern (maximum latitude) boundary.

    Returns:
    --------
    str
        WKT with BBOX[min_lat, min_lon, max_lat, max_lon] inserted at the end.

    Raises:
    -------
    ValueError
        If the WKT already contains BBOX or brackets are mismatched.

    Example:
    --------
    >>> insert_bbox_into_wkt(wkt, 8.0, 54.5, 15.2, 57.8)
    '...BBOX[54.5, 8.0, 57.8, 15.2]]'
    """
    if "BBOX[" in crs_wkt.upper():
        raise ValueError("WKT string already contains a BBOX definition.")

    # WKT 2.0 expects: [min_lat, min_lon, max_lat, max_lon]
    bbox_str = f"BBOX[{min_lat}, {min_lon}, {max_lat}, {max_lon}]"

    last_bracket_index = crs_wkt.rfind("]")
    if last_bracket_index == -1:
        raise ValueError("WKT format error: unmatched brackets")

    return crs_wkt[:last_bracket_index] + f", {bbox_str}" + crs_wkt[last_bracket_index:]


class WriteZarrTask(luigi.Task):
    """
    Task to write data to Zarr format using xarray from the Kerchunk JSON file.

    Parameters
    ----------
    start_year : int
        The start year for which to write the Zarr file.
    end_year : int
        The end year for which to write the Zarr file.
    data_kind : str
        The kind of data to write the Zarr file for, either 'hourly' or '5_minutes'.
    chunking : dict
        The chunking to use for the Zarr file, defaults to {'time': 128}.
    """

    start_year = luigi.IntParameter()
    end_year = luigi.IntParameter()
    data_kind = luigi.Parameter()
    chunking = luigi.DictParameter(default=dict(time=128))

    def requires(self):
        """
        Specifies the dependency on the KerchunkMultiYearTask.
        """
        return KerchunkMultiYearTask(
            start_year=self.start_year, end_year=self.end_year, data_kind=self.data_kind
        )

    def output(self):
        """
        Specifies the output target for the Zarr file.
        """
        return luigi.LocalTarget(
            DATA_PATH
            / "dst"
            / self.data_kind
            / "zarr"
            / f"{self.start_year}_{self.end_year}.zarr"
        )

    def run(self):
        """
        Writes data to Zarr format using xarray from the Kerchunk JSON file.
        """
        input_path = Path(self.input().path)
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        dask_distributed_local_core_fraction = 0.9
        dask_distributed_local_memory_fraction = 0.5

        if dask_distributed_local_core_fraction > 0.0:
            # Only run this block if dask.distributed is available
            # get the number of system cores
            n_system_cores = os.cpu_count()
            # compute the number of cores to use
            n_local_cores = int(dask_distributed_local_core_fraction * n_system_cores)
            # get the total system memory
            total_memory = psutil.virtual_memory().total
            # compute the memory per worker
            memory_per_worker = (
                total_memory / n_local_cores * dask_distributed_local_memory_fraction
            )

            logger.info(
                f"Setting up dask.distributed.LocalCluster with {n_local_cores} cores and {memory_per_worker / 1024 / 1024:0.0f} MB of memory per worker"
            )

            cluster = LocalCluster(
                n_workers=n_local_cores,
                threads_per_worker=1,
                memory_limit=memory_per_worker,
            )

        # print the dashboard link
        logger.info(f"Dashboard link: {cluster.dashboard_link}")

        # Open the dataset using the Kerchunk JSON file
        ds = xr.open_zarr(f"reference::{input_path}", consolidated=False).chunk(
            self.chunking
        )

        # all values for the projection data-array (crs) are the same, we only need to keep one
        ds["crs"] = ds.crs.isel(time=0)

        crs_wkt = ds.crs.attrs["crs_wkt"]

        # for cartopy to be able to use the projection for plotting we need to
        # add the bounds to the projection
        lat_min = float(ds["lat"].min())
        lat_max = float(ds["lat"].max())
        lon_min = float(ds["lon"].min())
        lon_max = float(ds["lon"].max())
        crs_wkt = insert_bbox_into_wkt(
            crs_wkt, min_lat=lat_min, min_lon=lon_min, max_lat=lat_max, max_lon=lon_max
        )

        # update the crs_wkt attribute with the new WKT string
        ds.crs.attrs["crs_wkt"] = crs_wkt

        # also the projection WKT states that the x-units are in 1000m, but the
        # actual coordinates are in m, so we need to scale the values by 1000
        # and change the units of the x/y variables to km
        ds["x"] = ds.x / 1000
        ds["y"] = ds.y / 1000
        ds.x.attrs["units"] = "km"
        ds.y.attrs["units"] = "km"

        # add meta info about the zarr dataset creation
        date = datetime.datetime.now().isoformat()
        ds.attrs["zarr_creation"] = (
            "created with mlcast_dataset_radklim "
            f"(https://github.com/mlcast-community/mlcast-dataset-radklim) on {date} "
            "by Leif Denby (lcd@dmi.dk)"
        )
        ds.attrs["zarr_dataset_version"] = __version__

        # Write the dataset to Zarr format
        ds.to_zarr(output_path, mode="w", consolidated=True)


def main():
    """
    Main method to execute the KerchunkTask for a single year.
    """
    luigi.build(
        [WriteZarrTask(start_year=2020, end_year=2021, data_kind="hourly")],
        local_scheduler=True,
    )


if __name__ == "__main__":
    main()
