import requests
import os

import dotenv

dotenv.load_dotenv()


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


api_key = os.environ.get("OPENAPI_KEY")

d = {
    "specversion": "1.0",
    "type": "nl.knmi.dataplatform.file.created.v1",
    "source": "https://dataplatform.knmi.nl",
    "id": "4170e3f4-89f5-fa8f-4168-7c3eac0a24ea",
    "time": "2024-02-13T12:40:33Z",
    "datacontenttype": "application/json",
    "data": {
        "datasetName": "radar_echotopheight_5min",
        "datasetVersion": "1.0",
        "filename": "RAD_NL25_ETH_NA_202402131235.h5",
        "url": "https://api.dataplatform.knmi.nl/open-data/v1/datasets/radar_echotopheight_5min/versions/1.0/files/RAD_NL25_ETH_NA_202402131235.h5/url",
    },
}

dataset_name = d["data"]["datasetName"]
dataset_url = d["data"]["url"]
dataset_version = d["data"]["datasetVersion"]
latest_file = d["data"]["filename"]

api = OpenDataAPI(api_key)
file_url = api.get_file_url(dataset_name, dataset_version, latest_file)

print(file_url)
