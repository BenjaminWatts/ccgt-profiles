from datetime import datetime, timedelta, timezone
import os
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import List
from pydantic import BaseModel, Field
import pandas as pd

FUEL_TYPE = 'CCGT'

# response structure

class ResponseData(BaseModel):
    startTime: datetime
    fuelType: str = FUEL_TYPE
    generation: int
    
class Response(BaseModel):
    data: List[ResponseData]
    
    def _to_series(self):
        return pd.Series([x.generation for x in self.data], index=[x.startTime for x in self.data])

# download all CCGT generation data
# https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST?settlementDateFrom=2022-06-20&settlementDateTo=2022-06-21&fuelType=CCGT

START_DATE = datetime(2017, 1, 1).replace(tzinfo=timezone.utc)

CURRENT_DATE = (
    datetime.now()
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .replace(tzinfo=timezone.utc)
)

# in parallel download each day's data and save to disk in the folder data/history

def download_data(date):
    url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST?settlementDateFrom={date.strftime('%Y-%m-%d')}&settlementDateTo={date.strftime('%Y-%m-%d')}&fuelType=CCGT"
    response = requests.get(url)
    if response.status_code == 200:
        file_path = f"data/history/{date.strftime('%Y-%m-%d')}.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        series = Response(**response.json())._to_series()
        series.to_json(file_path, date_format='iso', orient='index')

    else:
        print(f"Failed to download data for {date.strftime('%Y-%m-%d')}")

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

with ThreadPoolExecutor(max_workers=30) as executor:
    executor.map(download_data, daterange(START_DATE, CURRENT_DATE))