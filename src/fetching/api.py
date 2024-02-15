import logging
import os
import sys
import asyncio
import requests

import xarray as xr

from typing import List, Dict
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


class OpenDataAPI:
    def __init__(
        self, dataset_name: str, dataset_version: str, bulk_download: bool = False
    ):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"

        self.dataset_name = dataset_name
        self.dataset_version = dataset_version

        # Check if bulk download is required because the documentation is ambiguous
        if bulk_download:
            api_token = os.environ.get("OPENAPI_BULK_KEY")
        else:
            api_token = os.environ.get("OPENAPI_KEY")

        self.headers = {"Authorization": api_token}
        self.session = None

    def __get_data(self, url, params=None):
        # If a session is already created, use it
        if self.session is not None:
            return self.session.get(url, headers=self.headers, params=params).json()

        return requests.get(url, headers=self.headers, params=params).json()

    def list_files(self, params: dict) -> Dict:
        return self.__get_data(
            f"{self.base_url}/datasets/{self.dataset_name}/versions/{self.dataset_version}/files",
            params=params,
        )

    async def get_all_files(self, max_keys: int = 500):
        # Store the session token
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        filenames = []
        filesizes = []
        next_page_token = None

        # Retrieve all file names and file sizes
        while True:
            response = self.list_files(
                {"maxKeys": str(max_keys), "nextPageToken": next_page_token}
            )

            dataset_files: List[Dict] = response.get("files")
            filenames.extend(map(lambda x: x.get("filename"), dataset_files))
            filesizes.extend(file["size"] for file in dataset_files)

            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                logger.info("Retrieved names of all dataset files")
                break

        logger.info(f"Number of files to download: {len(filenames)}")

        worker_count = get_max_worker_count(filesizes)
        executor = ThreadPoolExecutor(max_workers=worker_count)

        # Download all files
        futures = []
        for filename in filenames:
            futures.append(executor.submit(self.get_file, filename))

        # Wait for all files to be downloaded
        results = await asyncio.gather(*futures)
        logger.info(f"Finished '{self.dataset_name}' dataset download")

        # Warn if any files failed to download
        failed_downloads = list(filter(lambda x: not x[0], results))
        if len(failed_downloads) > 0:
            logger.warning("Failed to download the following dataset files:")
            logger.warning(list(map(lambda x: x[1], failed_downloads)))

    def get_last_file(self) -> xr.Dataset:
        # sort the files in descending order and only retrieve the first file
        params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
        response = self.list_files(params)
        if "error" in response:
            logger.error(f"Unable to retrieve list of files: {response['error']}")
            sys.exit(1)

        latest_file = response["files"][0].get("filename")
        logger.info(f"Latest file is: {latest_file}")

        return self.get_file(latest_file)

    def get_file(self, file_name: str) -> xr.Dataset:
        temporary_download_url = self.get_file_url(file_name)
        return self.download_file_into_xarray(temporary_download_url)

    def get_file_url(self, file_name: str) -> xr.Dataset:

        res = self.__get_data(
            f"{self.base_url}/datasets/{self.dataset_name}/versions/{self.dataset_version}/files/{file_name}/url"
        )

        logger.info(f"Downloading file from {res['temporaryDownloadUrl']}")

        return res["temporaryDownloadUrl"]

    def download_file_into_xarray(self, download_url: str) -> xr.Dataset:
        try:
            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()

                # Save into temporary file
                with open("/tmp/temp.nc", "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Open the file using xarray
            ds = xr.open_dataset("/tmp/temp.nc", cache=True)

            # Delete the temporary file
            os.remove("/tmp/temp.nc")

        except Exception:
            logger.exception("Unable to download file using download URL")
            sys.exit(1)

        return ds


def get_max_worker_count(filesizes):
    size_for_threading = 10_000_000  # 10 MB

    average = sum(filesizes) / len(filesizes)

    # to prevent downloading multiple half files in case of a network failure with big files
    if average > size_for_threading:
        threads = 1
    else:
        threads = 10
    return threads


def main():
    # Dataset Name and Version should be provided by the user
    dataset_name = "Actuele10mindataKNMIstations"
    dataset_version = "2"

    api = OpenDataAPI()

    ds = api.get_last_file()
    df = ds.to_dataframe()

    print(ds)
    print(df.head())


if __name__ == "__main__":
    main()
