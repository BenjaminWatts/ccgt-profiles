import json
import os
from typing import List, Optional
import pandas as pd
from pydantic import BaseModel
from datetime import date, datetime, timedelta, timezone
from pytz import utc
import requests
from concurrent.futures import ThreadPoolExecutor
import random

API_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

FUEL_TYPE = "CCGT"


def get_gas_bm_units():
    """get bm units from Elexon that are gas plants"""
    resp = requests.get(f"{API_BASE}/reference/bmunits/all", timeout=TIMEOUT)
    if resp.status_code != 200:
        raise Exception(f"Error: {resp.status_code}")
    all_units = [BmUnitResponseValue(**v) for v in resp.json()]
    ccgts = set()
    for unit in all_units:
        if not unit.fpnFlag:
            continue
        if not unit.elexonBmUnit:
            continue
        if unit.fuelType == FUEL_TYPE:
            ccgts.add(unit.elexonBmUnit)

    print(f"Found {len(ccgts)} CCGT units")
    return list(ccgts)


class ResponseValue(BaseModel):
    bmUnit: str
    timeFrom: datetime
    timeTo: datetime
    levelFrom: int
    levelTo: int


class RequestParams(BaseModel):
    start: datetime  # = Field(alias="from")
    end: datetime  # = Field(alias="to")
    bmUnit: List[str]

    def model_dump(self):
        return {
            "to": self.end.isoformat(),
            "from": self.start.isoformat(),
            "bmUnit": self.bmUnit,
        }


TIMEOUT = 30


def get_average_value(params: RequestParams, values: List[ResponseValue]):
    """calculate the timeweighted average of the values"""
    data = {}
    for v in values:
        data[v.timeFrom] = v.levelFrom
        data[v.timeTo] = v.levelTo
    series = pd.Series(data)
    if series.empty:
        return 0
    res = series.resample("30min").mean()
    return float(res[params.start])


def group_by_bm_unit(params: RequestParams, values: List[ResponseValue]):
    """group the values by bm unit"""
    total = 0
    data = {bmUnit: [] for bmUnit in params.bmUnit}
    for v in values:
        data[v.bmUnit].append(v)
    for values in data.values():
        total += get_average_value(params, values)
    return total


def get_data(params: RequestParams, prefix: str):
    resp = requests.get(
        f"{API_BASE}/datasets/{prefix}/stream",
        params=params.model_dump(),
        timeout=TIMEOUT,
    )
    if resp.status_code != 200:
        raise Exception(f"Error: {resp.status_code}")
    raw_values = [ResponseValue(**v) for v in resp.json()]
    return group_by_bm_unit(params, raw_values)


def get_mels_data(params: RequestParams):
    """get the PN data for the relevant BM Units"""
    return get_data(params, "MELS")


def get_pn_data(params: RequestParams):
    """get the PN data for the relevant BM Units"""
    return get_data(params, "PN")


class SettlementPeriodTotals(BaseModel):
    dt: str
    mels: float
    pn: float


def get_settlement_period(start: datetime, bmUnits: List[str]):
    end = start + timedelta(minutes=30)
    params = RequestParams(start=start, end=end, bmUnit=bmUnits)
    with ThreadPoolExecutor() as executor:
        mels_future = executor.submit(get_mels_data, params)
        pn_future = executor.submit(get_pn_data, params)
        mels = mels_future.result()
        pn = pn_future.result()
    totals = SettlementPeriodTotals(dt=start.isoformat(), mels=mels, pn=pn)
    return totals


def get_date(date: date, bmUnits: List[str]):
    output_fp = f"data/pn_history/{date.isoformat()}.json"
    if os.path.exists(output_fp):
        print(f"Skipping {date}")
        return

    starts = pd.date_range(start=date, periods=48, freq="30min", tz=utc).to_pydatetime()

    with ThreadPoolExecutor() as executor:
        values = list(
            executor.map(lambda start: get_settlement_period(start, bmUnits), starts)
        )

    as_dict = [v.model_dump() for v in values]

    with open(output_fp, "w") as f:
        print(f"Writing to {output_fp}")
        json.dump(as_dict, f, indent=2)


class BmUnitResponseValue(BaseModel):
    fuelType: Optional[str]
    fpnFlag: Optional[bool]
    elexonBmUnit: Optional[str]


START_DATE = datetime(2024, 9, 17).replace(tzinfo=timezone.utc)

CURRENT_DATE = (
    datetime.now()
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .replace(tzinfo=timezone.utc)
)


def daterange(start_date, end_date):
    dates = [start_date + timedelta(n) for n in range(int((end_date - start_date).days))]
    random.shuffle(dates)
    for date in dates:
        yield date


bmUnits = get_gas_bm_units()
with ThreadPoolExecutor(max_workers=30) as executor:
    executor.map(
        lambda date: get_date(date, bmUnits), daterange(START_DATE, CURRENT_DATE)
    )
