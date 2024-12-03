import datetime
import json
from pathlib import Path

import kerchunk.combine
import kerchunk.hdf
import luigi
import xarray as xr
from loguru import logger

from .__init__ import __version__
from .config import DATA_PATH
from .source import UntarYearTask


class KerchunkTask(luigi.Task):
    """
    Task to build a JSON for Zarr using Kerchunk from untarred netCDF files.

    Parameters
    ----------
    start_year : int
        The start year for which to build the Zarr JSON.
    end_year : int
        The end year for which to build the Zarr JSON.
    data_kind : str
        The kind of data to build the Zarr JSON for, either 'hourly' or '5_minutes'.

    """

    start_year = luigi.IntParameter()
    end_year = luigi.IntParameter()
    data_kind = luigi.Parameter()

    def requires(self):
        """
        Specifies the dependency on the UntarTask.
        """
        tasks = {
            year: UntarYearTask(year=year, data_kind=self.data_kind)
            for year in range(self.start_year, self.end_year + 1)
        }
        return tasks

    def output(self):
        """
        Specifies the output target for the Kerchunk JSON file.
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
        Uses Kerchunk to build a JSON for Zarr from the untarred netCDF files.
        """
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        references = []

        for year, input_task in self.input().items():
            # Collect all netCDF files in the untarred directory
            input_path = Path(input_task.path)
            netcdf_files = list(input_path.glob("**/*.nc"))

            logger.info(f"Found {len(netcdf_files)} netCDF files for year {year}")

            # Use Kerchunk to build the JSON
            for nc_file in netcdf_files:
                with open(nc_file, "rb") as f:
                    h5chunks = kerchunk.hdf.SingleHdf5ToZarr(f, str(nc_file))
                    file_refs = h5chunks.translate()
                    references.append(file_refs)

        # combine the references
        mzz = kerchunk.combine.MultiZarrToZarr(
            references, concat_dims=["time"], identical_dims=["lat", "lon"]
        )
        combined_references = mzz.translate()

        # Write the combined references to the output JSON file
        with output_path.open("w") as f:
            json.dump(combined_references, f)


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
        Specifies the dependency on the KerchunkTask.
        """
        return KerchunkTask(
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

        # Open the dataset using the Kerchunk JSON file
        ds = xr.open_zarr(f"reference::{input_path}", consolidated=False).chunk(
            self.chunking
        )

        # add meta info about the zarr dataset creation
        date = datetime.datetime.now().isoformat()
        version = __version__
        ds.attrs["zarr-creation"] = (
            "created with mlcast_dataset_radklim "
            f"(https://github.com/mlcast-community/mlcast-dataset-radklim) {version} on {date} "
            "by Leif Denby (lcd@dmi.dk)"
        )

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
