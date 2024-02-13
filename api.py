import logging
import os
import sys

import requests

import xarray as xr
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


class OpenDataAPI:
    def __init__(self, api_token: str):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": api_token}

    def __get_data(self, url, params=None):
        return requests.get(url, headers=self.headers, params=params).json()

    def list_files(self, dataset_name: str, dataset_version: str, params: dict):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
            params=params,
        )

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filename}")


def download_file_into_xarray(download_url: str) -> xr.Dataset:
    try:
       with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            
            # Save into temporary file
            with open("/tmp/temp.nc", "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            # Open the file using xarray
            ds = xr.open_dataset("/tmp/temp.nc", cache=True, engine="h5netcdf")
            
            # Delete the temporary file
            os.remove("/tmp/temp.nc")
            
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)
        
    return ds

def main():
    api_key = os.environ.get("OPENAPI_KEY")
    dataset_name = "Actuele10mindataKNMIstations"
    dataset_version = "2"
    logger.info(f"Fetching latest file of {dataset_name} version {dataset_version}")

    api = OpenDataAPI(api_token=api_key)

    # sort the files in descending order and only retrieve the first file
    params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
    response = api.list_files(dataset_name, dataset_version, params)
    if "error" in response:
        logger.error(f"Unable to retrieve list of files: {response['error']}")
        sys.exit(1)

    latest_file = response["files"][0].get("filename")
    logger.info(f"Latest file is: {latest_file}")

    # fetch the download url and download the file
    response = api.get_file_url(dataset_name, dataset_version, latest_file)
    # download_file_from_temporary_download_url(response["temporaryDownloadUrl"], latest_file)
    
    ds = download_file_into_xarray(response["temporaryDownloadUrl"])
    df = ds.to_dataframe()
    
    print(ds)
    print(df.head())


if __name__ == "__main__":
    main()