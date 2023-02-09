from requests import exceptions
from requests import models, get, Session, adapters
import pandas as pd

from pathlib import Path

from typing import Tuple, List, Dict, Any

from abc import abstractmethod, ABC


class SurflineRequestBase(ABC):
    def __init__(
        self,
        base_url: str = "https://services.surfline.com/kbyg/spots/forecasts/"
    ):
        self.base_url = base_url 

    def get_request_from_url(
        self,
        url: str,
        num_retries: int = 0,
        timeout_seconds: int = 5,
        retry_status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    ) -> models.Response:

        if num_retries == 0:
            return get(url, timeout=timeout_seconds)

        _retry = adapters.Retry(
            total=num_retries, backoff_factor=2, status_forcelist=retry_status_forcelist
        )

        _adapter = adapters.HTTPAdapter(max_retries=_retry)

        with Session as s:
            if url.startswith("https"):
                s.mount("https://", _adapter)
            else:
                s.mount("http://", _adapter)

            return s.get(
                url,
                timeout=timeout_seconds,
            )

    def get_surfline_forecast_request(
        self, 
        fcst_type: str, 
        spot_id: str, 
        days: int = 1, 
        interval_hours: int=1
    ):
        """_summary_

        Args:
            fcst_type (str): Can be wave, tides, or wind
            spot_id (int): Spot ID
        """

        try:
            web_page = self.get_request_from_url(
                url=self.base_url
                + fcst_type
                + f"?spotId={spot_id}" 
                + f"&days={days}"
                + f"&intervalHours={interval_hours}"
            )
        except exceptions.HTTPError as e:
            raise e

        document = web_page.json()

        return document

    @abstractmethod
    def parse_raw_json_data() -> pd.DataFrame:
        raise NotImplementedError("parse_raw_json_data function not implemented by base class")

    # @staticmethod
    # @abstractmethod
    # def get_spot_id_by_keyword(keyword: str) -> int:
    #     raise NotImplementedError("get_spot_id_by_keyword function not implemented by base class")
