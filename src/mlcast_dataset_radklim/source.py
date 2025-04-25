"""
Luigi Tasks for downloading and untarring the RADKLIM dataset.

Details on source netCDF files:
hourly acc:      https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/2001/RW2017.002_2001_netcdf.tar.gz
- stored in yearly .tar.gz files
- one netCDF file per month
- year 2021 is incorrect, use
                 https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/supplement/RW2017.002_2021_netcdf_supplement.tar.gz

5-min rainrate:  https://opendata.dwd.de/climate_environment/CDC/grids_germany/5_minutes/radolan/reproc/2017_002/netCDF/2001/YW2017.002_2001_netcdf.tar.gz
- stored in yearly .tar.gz files
- one netCDF file per day (2001/01/YW_2017.002_20010101.nc)
- year 2021 is incorrect, use
                 https://opendata.dwd.de/climate_environment/CDC/grids_germany/5_minutes/radolan/reproc/2017_002/netCDF/supplement/YW2017.002_2021_netcdf_supplement.tar.gz

"""
import tarfile
from pathlib import Path

import luigi
import requests
from loguru import logger
from tqdm import tqdm

from .config import DATA_PATH

BASE_URL_FORMAT = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/{data_kind}/radolan/reproc/2017_002/netCDF/"

HOURLY_URL_FORMAT = BASE_URL_FORMAT + "{year}/RW2017.002_{year}_netcdf.tar.gz"
FIVE_MINUTES_URL_FORMAT = BASE_URL_FORMAT + "{year}/YW2017.002_{year}_netcdf.tar.gz"

HOURLY_URL_SUPPLMENT_FORMAT = (
    BASE_URL_FORMAT + "supplement/RW2017.002_{year}_netcdf_supplement.tar.gz"
)
FIVE_MINUTES_URL_SUPPLMENT_FORMAT = (
    BASE_URL_FORMAT + "supplement/YW2017.002_{year}_netcdf_supplement.tar.gz"
)

YEAR_MIN = 2001
YEAR_MAX = 2024


def create_url(data_kind: str, year: int) -> str:
    """
    Create the URL for a given data kind and year.

    Parameters
    ----------

    data_kind : str
        The kind of data to download. Either 'hourly' or '5_minutes'.
    year : int
        The year for which to download the data.

    Returns:
    The URL for the specified data kind and year.
    """
    if data_kind not in ["hourly", "5_minutes"]:
        raise ValueError(f"Invalid data kind: {data_kind}")

    if year < YEAR_MIN or year > YEAR_MAX:
        raise ValueError(f"Invalid year: {year}")

    if data_kind == "hourly":
        if year == 2021:
            return HOURLY_URL_SUPPLMENT_FORMAT.format(data_kind=data_kind, year=year)
        else:
            return HOURLY_URL_FORMAT.format(data_kind=data_kind, year=year)
    elif data_kind == "5_minutes":
        if year == 2021:
            return FIVE_MINUTES_URL_SUPPLMENT_FORMAT.format(
                data_kind=data_kind, year=year
            )
        else:
            return FIVE_MINUTES_URL_FORMAT.format(data_kind=data_kind, year=year)
    else:
        raise ValueError(f"Invalid data kind: {data_kind}")


class DownloadTask(luigi.Task):
    """
    Task to download a source .tar.gz/.tar file for specific kind of data (hourly or 5_minutes)
    for a given year.

    Parameters
    ----------
    year : int
        The year for which the file should be downloaded.
    data_kind : str
        The kind of data to download. Either 'hourly' or '5_minutes'.

    """

    year = luigi.IntParameter()
    data_kind = luigi.Parameter()

    def _url(self):
        return create_url(data_kind=self.data_kind, year=self.year)

    def output(self):
        """
        Specifies the output target for the downloaded file.
        """
        fn = Path(self._url()).name
        return luigi.LocalTarget(DATA_PATH / "src" / self.data_kind / "compressed" / fn)

    def run(self):
        """
        Downloads the file from the URL and saves it to the output target.
        """
        url = create_url(data_kind=self.data_kind, year=self.year)

        response = requests.get(url, stream=True)
        # check response status code
        if response.status_code != 200:
            raise Exception(
                f"Failed to download file: {url} (status code: {response.status_code})"
            )
        total_size = int(response.headers.get("content-length", 0))
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("wb") as out_file, tqdm(
            desc=f"Downloading {self.year}",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                out_file.write(chunk)
                bar.update(len(chunk))


class UntarYearTask(luigi.Task):
    """
    Task to untar a downloaded file.

    Arguments:
    year -- The year for which the file should be untarred.
    """

    year = luigi.IntParameter()
    data_kind = luigi.Parameter(default="hourly")

    def requires(self):
        """
        Specifies the dependency on the DownloadTask.
        """
        return DownloadTask(year=self.year, data_kind=self.data_kind)

    def output(self):
        """
        Specifies the output target for the untarred directory.
        """
        return luigi.LocalTarget(
            DATA_PATH / "src" / self.data_kind / f"uncompressed/{self.year}"
        )

    def run(self):
        """
        Untars the downloaded file to the specified output directory.
        """
        output_path = Path(self.output().path)
        output_path.mkdir(parents=True, exist_ok=True)
        input_task = self.input()

        try:
            with tarfile.open(input_task.path, "r:gz") as tar:
                tar.extractall(path=output_path)
        except tarfile.ReadError as ex:
            # delete the output directory if the tar file is not valid
            output_path.rmdir()
            raise Exception(
                f"There was an error reading the tar file: {input_task.path}"
            ) from ex


class DownloadAllYearsTask(luigi.WrapperTask):
    """
    Task to download files for all years between start_year and end_year.

    Arguments:
    start_year -- The starting year for the download range.
    end_year -- The ending year for the download range.
    """

    start_year = luigi.IntParameter()
    end_year = luigi.IntParameter()

    def requires(self):
        """
        Specifies the list of DownloadTask for each year in the range.
        """
        return [
            DownloadTask(year=year)
            for year in range(self.start_year, self.end_year + 1)
        ]


class UntarAllYearsTask(luigi.WrapperTask):
    """
    Task to untar files for all years between start_year and end_year.

    Arguments:
    start_year -- The starting year for the untar range.
    end_year -- The ending year for the untar range.
    """

    start_year = luigi.IntParameter()
    end_year = luigi.IntParameter()

    def requires(self):
        """
        Specifies the list of UntarTask for each year in the range.
        """
        return [
            UntarYearTask(year=year)
            for year in range(self.start_year, self.end_year + 1)
        ]


@logger.catch
def main():
    luigi.build(
        [UntarAllYearsTask(start_year=2020, end_year=2021)], local_scheduler=True
    )


if __name__ == "__main__":
    main()
