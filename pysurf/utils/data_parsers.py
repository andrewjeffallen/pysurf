import pandas as pd
from web import SurflineRequestBase
import datetime
from typing import Dict
import pytz


class Waves(SurflineRequestBase):
    """
    Parser class for waves data
    """

    def parse_raw_json_data(self, spot_id: str) -> pd.DataFrame:

        json_data = self.get_surfline_forecast_request(
            fcst_type="wave", spot_id=spot_id
        )
        
        #TODO: this is ugly, try to reduce CPU cycles from Pandas if requesting larger datasets
        base_df = pd.DataFrame(json_data["data"]["wave"])
        surf_df = pd.json_normalize(base_df.surf)
        swell_df = pd.json_normalize(pd.json_normalize(base_df.swells)[0])
        df = base_df.join(surf_df).drop(columns=["surf", "swells"]).merge(swell_df)
        df.columns = df.columns.str.replace(".", "_").str.lower()
        df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

        return df
    
    
class Tides(SurflineRequestBase):
    """
    Parser class for tides data
    """

    def parse_raw_json_data(self, spot_id: str) -> pd.DataFrame:

        json_data = self.get_surfline_forecast_request(
            fcst_type="tides", spot_id=spot_id
        )
        df = pd.DataFrame(json_data["data"]["tides"])
        df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        
        return df

class Wind(SurflineRequestBase):
    """
    Parser class for wind data
    """

    def parse_raw_json_data(self, spot_id: str) -> pd.DataFrame:

        json_data = self.get_surfline_forecast_request(
            fcst_type="wind", spot_id=spot_id
        )
        df = pd.DataFrame(json_data["data"]["wind"])
        df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        
        return df