import luigi
import kerchunk.hdf
import json
from pathlib import Path
import xarray as xr
import fsspec
from loguru import logger

from .source import UntarTask, DATA_PATH


class KerchunkTask(luigi.Task):
    """
    Task to build a JSON for Zarr using Kerchunk from untarred netCDF files.

    Arguments:
    year -- The year for which the JSON should be built.
    """
    year = luigi.IntParameter()

    def requires(self):
        """
        Specifies the dependency on the UntarTask.
        """
        return UntarTask(year=self.year)

    def output(self):
        """
        Specifies the output target for the Kerchunk JSON file.
        """
        return luigi.LocalTarget(DATA_PATH / f'dst/jsons/RW2017.002_{self.year}_netcdf.json')

    def run(self):
        """
        Uses Kerchunk to build a JSON for Zarr from the untarred netCDF files.
        """
        input_path = Path(self.input().path)
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Collect all netCDF files in the untarred directory
        netcdf_files = list(input_path.glob('**/*.nc'))

        # Use Kerchunk to build the JSON
        references = []
        for nc_file in netcdf_files:
            with open(nc_file, 'rb') as f:
                h5chunks = kerchunk.hdf.SingleHdf5ToZarr(f, str(nc_file))
                references.append(h5chunks.translate())

        # Combine all references into a single JSON
        combined_references = {k: v for d in references for k, v in d.items()}

        # Write the combined references to the output JSON file
        with output_path.open('w') as f:
            json.dump(combined_references, f)
        

class WriteZarrTask(luigi.Task):
    """
    Task to write data to Zarr format using xarray from the Kerchunk JSON file.

    Arguments:
    year -- The year for which the Zarr file should be written.
    """
    year = luigi.IntParameter()
    chunking = luigi.DictParameter(default=dict(time=128))

    def requires(self):
        """
        Specifies the dependency on the KerchunkTask.
        """
        return KerchunkTask(year=self.year)

    def output(self):
        """
        Specifies the output target for the Zarr file.
        """
        return luigi.LocalTarget(f'data/dst/zarr/RW2017.002_{self.year}_netcdf.zarr')

    def run(self):
        """
        Writes data to Zarr format using xarray from the Kerchunk JSON file.
        """
        input_path = Path(self.input().path)
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Open the dataset using the Kerchunk JSON file
        ds = xr.open_zarr(f"reference::{input_path}", consolidated=False).chunk(self.chunking)

        # Write the dataset to Zarr format
        ds.to_zarr(output_path, mode='w', consolidated=True)

def main():
    """
    Main method to execute the KerchunkTask for a single year.
    """
    luigi.build([WriteZarrTask(year=2020)], local_scheduler=True)

if __name__ == '__main__':
    main()