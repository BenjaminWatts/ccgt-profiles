from datetime import datetime
import os
from typing import List, Tuple
import pandas as pd
import concurrent.futures
import matplotlib.pyplot as plt
import numpy as np

HISTORY_FP = 'data/history'
DATA_FREQ_MINUTES = 5
EXPECTED_RECORDS_PER_DAY = 24 * 60 / DATA_FREQ_MINUTES
CURRENT_YEAR = datetime.now().year
COMBINED_SERIES_FP = 'data/combined_series.json'
EXCEL_FP = 'data/combined_excel.xlsx'

# convert data to excel format

def convert_to_excel(series: pd.Series):
    ''' Convert the DataFrame to an Excel file '''
    series.to_frame('ccgt_generation').to_excel(EXCEL_FP)

# Helper function to read a json file into a pd.Series
def read_series(filepath):
    return pd.read_json(filepath, typ='series')


# Function to open all .json files and combine the series
def open_all():
    if os.path.exists(COMBINED_SERIES_FP):
        print("Opening combined series")
        combined_series = pd.read_json(COMBINED_SERIES_FP, typ='series')
        combined_series.index = pd.to_datetime(combined_series.index)  # Parse dates in the index
        return combined_series
    
    print("Opening all series")
    series_list = []
    
    # Limit the number of concurrent threads
    max_workers = 8  # You can adjust this number based on your system
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        filenames = [f for f in os.listdir(HISTORY_FP) if f.endswith('.json')]
        total_files = len(filenames)
        
        for idx, filename in enumerate(filenames):
            filepath = os.path.join(HISTORY_FP, filename)
            futures.append(executor.submit(read_series, filepath))
            print(f"Submitted {idx + 1}/{total_files}: {filename}")

        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                series = future.result()
                series.index = pd.to_datetime(series.index)  # Parse dates in the index
                series_list.append(series)
                print(f"Completed {idx + 1}/{total_files}")
            except Exception as e:
                print(f"Error processing file: {e}")

    if series_list:
        print("Combining series...")
        combined_series = pd.concat(series_list, axis=1)
        combined_series = combined_series.mean(axis=1)  # Take the average where overlaps exist, or keep NaN where there's no data
        
        # write combined_series to a file
        combined_series.to_json(COMBINED_SERIES_FP)
        print("All series opened and combined")
        return combined_series
    else:
        raise Exception("No series to combine")


# Function to split the series into separate years
def split_calendar_years(series: pd.Series):
    ''' Split the series into separate series for each calendar year '''
    return series.groupby(series.index.year)


# Function to print basic stats for a given series
def print_stats(series: pd.Series):
    print(f"Mean: {series.mean()}")
    print(f"Median: {series.median()}")
    print(f"Max: {series.max()}")
    print(f"Min: {series.min()}")
    print(f"Std: {series.std()}")


# Calculate expected records for a given year
def records_per_year(year: int):
    if year % 4 == 0:  # Leap year
        return 366 * EXPECTED_RECORDS_PER_DAY
    else:
        return 365 * EXPECTED_RECORDS_PER_DAY


# Calculate capacity factor for a given year and series
def capacity_factor_by_year(year, series: pd.Series) -> pd.Series:
    ''' Calculate the relevant statistics for the series '''
    capacity = series.max()  # Capacity is the maximum value in the series
    
    # Check if the number of records is as expected
    if CURRENT_YEAR > year:
        expected_records = records_per_year(year)
        if len(series) != expected_records:
            print(f"Missing records for {year}, expected {expected_records}, got {len(series)}")
    
    # Calculate capacity factor (generation as a fraction of capacity)
    capacity_factor = series / capacity
    return capacity_factor


PERCENTILES = [x/100 for x in range(1, 100, 1)]

# Plot distribution of capacity factors for each year
def plot_capacity_factors(year_cf_tuples: List[Tuple[int, pd.Series]]):
    ''' Plot the distribution of capacity factors for each year '''
    plt.figure(figsize=(12, 8))

    for year, cf in year_cf_tuples:
        stats = cf.describe(percentiles=PERCENTILES)
        percentiles_only = stats.loc[[f'{int(p*100)}%' for p in PERCENTILES]]
        plt.plot(percentiles_only.index, percentiles_only.values, label=str(year))

    plt.title('Distribution of Capacity Factors by Year')
    plt.xlabel('Hours within Calendar Year')
    plt.ylabel('Fleet Capacity Factor')
    plt.legend(loc='upper right')
    
    # Reduce the number of x-axis labels
    plt.xticks(ticks=np.arange(0, len(PERCENTILES), step=10), labels=[f'{int(PERCENTILES[i]*100)}%' for i in range(0, len(PERCENTILES), 10)])
    
    # Save plot in high resolution for LinkedIn
    plt.savefig("capacity_factors_distribution_linkedin.png", dpi=300)


def __main__():
    series = open_all()
    convert_to_excel(series)
    
    yearly_series = split_calendar_years(series)
    
    capacity_factors = []
    for year, year_series in yearly_series:
        capacity_factors.append((year, capacity_factor_by_year(year, year_series)))
    
    plot_capacity_factors(capacity_factors)

if __name__ == '__main__':
    __main__()