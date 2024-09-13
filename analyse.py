from datetime import datetime
import os
from typing import List, Tuple
import pandas as pd



# open all the pd.Series objects that are serialised as json in data/history and combine them into a single series

HISTORY_FP = 'data/history'

import concurrent.futures

def read_series(filepath):
    return pd.read_json(filepath, typ='series')

def open_all():
    print("Opening all series")
    combined_series = pd.Series(dtype=float)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for filename in os.listdir(HISTORY_FP):
            if filename.endswith('.json'):
                filepath = os.path.join(HISTORY_FP, filename)
                futures.append(executor.submit(read_series, filepath))
        
        for future in concurrent.futures.as_completed(futures):
            series = future.result()
            combined_series = combined_series.add(series)
            
    print("All series opened")
    
    return combined_series
    
def split_calendar_years(series: pd.Series):
    ''' Split the series into separate series for each calendar year '''
    return series.groupby(series.index.year)

def print_stats(series: pd.Series):
    # for debugging purposes print the mean, median, max, min, and standard deviation of the series
    pass

DATA_FREQ_MINUTES = 5
EXPECTED_RECORDS_PER_DAY = 24 * 60 / DATA_FREQ_MINUTES

def records_per_year(year: int):
    ''' calculate the expected records for a complete calendar year'''

CURRENT_YEAR = datetime.now().year


def capacity_factor_by_year(year, series: pd.Series) -> pd.Series:
    ''' calculate the relevant statistics for the series '''
    # assume capacity is that maximum value in the series
    capacity = max(series)
    # now check we have the correct number of records for the year
    if CURRENT_YEAR > year:
        expected_records = records_per_year(year)
        if len(series) != expected_records:
            print(f"Missing records for {year}, expected {expected_records}, got {len(series)}")
            
    # now calculate the generation in each hour as a fraction of the estimated capacity
    capacity_factor: pd.Series = series / capacity
    
    return capacity_factor.values()    


def plot_capacity_factors(year_cf_tuples: List[Tuple[int, pd.Series]]):
    ''' Plot the capacity factors for each year on a matplotlib plot with a colour shading that is darkest in the first year and gets progresively lighter '''
    pass
    

def __main__():
    series = open_all()
    yearly_series = split_calendar_years(series)
    capacity_factors = [(year, capacity_factor_by_year(year, year_series)) for year, year_series in yearly_series]
    plot_capacity_factors(capacity_factors)

        
if __name__ == '__main__':
    __main__()