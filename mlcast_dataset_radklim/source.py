import luigi
import requests
import tarfile
from pathlib import Path
from tqdm import tqdm
from loguru import logger

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/"
URL_FORMAT = BASE_URL + "{year}/RW2017.002_{year}_netcdf.tar.gz"
# from readme.txt in the base url above:
# The netCDF data for the 2021
# (https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/2021/)
# are incorrect and shouldn't be used. The corrected data can be found in the
# folder "supplement"
# (https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/reproc/2017_002/netCDF/supplement/)

SUPPLEMENT_URL = BASE_URL + "supplement/RW2017.002_2021_netcdf_supplement.tar.gz"
DATA_PATH = Path("data/")

class DownloadTask(luigi.Task):
    """
    Task to download a file from a specified URL.

    Arguments:
    year -- The year for which the file should be downloaded.
    """
    year = luigi.IntParameter()

    def output(self):
        """
        Specifies the output target for the downloaded file.
        """
        return luigi.LocalTarget(DATA_PATH / f'src/compressed/RW2017.002_{self.year}_netcdf.tar.gz')

    def run(self):
        """
        Downloads the file from the URL and saves it to the output target.
        """
        if self.year == 2021:
            url = SUPPLEMENT_URL
        else:
            url = URL_FORMAT.format(year=self.year)
        
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('wb') as out_file, tqdm(
            desc=f'Downloading {self.year}',
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                out_file.write(chunk)
                bar.update(len(chunk))


class UntarTask(luigi.Task):
    """
    Task to untar a downloaded file.

    Arguments:
    year -- The year for which the file should be untarred.
    """
    year = luigi.IntParameter()

    def requires(self):
        """
        Specifies the dependency on the DownloadTask.
        """
        return DownloadTask(year=self.year)

    def output(self):
        """
        Specifies the output target for the untarred directory.
        """
        return luigi.LocalTarget(DATA_PATH / f'src/uncompressed/RW2017.002_{self.year}_netcdf')

    def run(self):
        """
        Untars the downloaded file to the specified output directory.
        """
        output_path = Path(self.output().path)
        output_path.mkdir(parents=True, exist_ok=True)
        with tarfile.open(self.input().path, 'r:gz') as tar:
            tar.extractall(path=output_path)
            
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
        return [DownloadTask(year=year) for year in range(self.start_year, self.end_year + 1)]
    
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
        return [UntarTask(year=year) for year in range(self.start_year, self.end_year + 1)]

    

@logger.catch
def main():
    luigi.build([UntarAllYearsTask(start_year=2020, end_year=2021)], local_scheduler=True)

if __name__ == '__main__':
    main()