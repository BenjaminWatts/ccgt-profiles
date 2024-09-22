
from typing import List, Tuple
import numpy as np
import pandas as pd, os
from matplotlib import pyplot as plt

def open_actual():
    ''' open the actual data which has an interval of 5 minutes'''
    fp = 'data/combined_series.json'
    ser = pd.read_json(fp, typ='series')
    ser.index = pd.to_datetime(ser.index)
    if ser.index.tz is None:
        ser.index = ser.index.tz_localize('UTC')
    ser = ser.resample('30min').mean()
    return ser.to_frame('post_bm')

def open_pn():
    ''' open the pn series which has an interval of 30 minutes'''
    dir_fp = 'data/pn_history'
    series = None
    for f in os.listdir(dir_fp):
        fp = os.path.join(dir_fp, f)
        df = pd.read_json(fp, convert_dates=['dt'])
        df.index = df['dt']
        del df['dt']
        if series is None:
            series = df['pn']
        else:
            series = series.combine_first(df['pn'])

    series = series.sort_index()
    return series.to_frame('pn')

def combine_series(actual, pn):
    df = pd.concat([actual, pn], axis=1)
    # count_before = len(df)
    df = df.dropna()
    # count_after = len(df)
    # print(f"Removed {count_before - count_after} rows with NaN values - likely no data for PN yet")
    return df

PERCENTILES = [x/100 for x in range(1, 100, 1)]


def print_stats(year, series: pd.Series):
    ''' plot the statistics of the series for this year'''
    print(f"Year: {year}")
    print(f"Mean: {series.mean()}")
    print(f"Median: {series.median()}")
    print(f"Max: {series.max()}")
    print(f"Min: {series.min()}")
    print(f"Std: {series.std()}")

def capacity_factor_by_year(year, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    ''' Calculate percentiles for post_bm and pn for a specific year '''
    df_year = df[df.index.year == year]
    capacity = df_year.max().max()  # Use the maximum value in both series as capacity
    
    # Calculate capacity factors
    capacity_factor_post_bm = (df_year['post_bm'] / capacity).dropna()
    capacity_factor_pn = (df_year['pn'] / capacity).dropna()
    
    # Calculate percentiles for both series
    percentiles_post_bm = capacity_factor_post_bm.describe(percentiles=PERCENTILES).loc[[f'{int(p*100)}%' for p in PERCENTILES]]
    percentiles_pn = capacity_factor_pn.describe(percentiles=PERCENTILES).loc[[f'{int(p*100)}%' for p in PERCENTILES]]
    
    return percentiles_post_bm, percentiles_pn


def split_calendar_years(df: pd.DataFrame):
    return df.groupby(df.index.year)

def plot_faceted_capacity_factors(year_cf_tuples: List[Tuple[int, pd.Series, pd.Series]]):
    ''' Plot the percentiles of capacity factors for both post_bm and pn for all years, faceted in a 2x layout, with percentiles below 75% '''
    
    # Truncate percentiles to below 75%
    trunc_percentiles = PERCENTILES[:int(0.75 * len(PERCENTILES))]  # Only keep percentiles below 75%
    
    num_years = len(year_cf_tuples)
    
    # Set up a 2-column layout for faceting (rows and cols)
    cols = 2  # Number of columns
    rows = (num_years + 1) // cols  # Ensure we have enough rows to accommodate the years
    
    fig, axes = plt.subplots(rows, cols, figsize=(12, rows * 3), sharex=True)
    axes = axes.flatten()  # Ensure axes is flat and iterable
    
    for i, (year, post_bm_cf, pn_cf) in enumerate(year_cf_tuples):
        ax = axes[i]
        
        # Truncate both series to below 75%
        post_bm_cf_truncated = post_bm_cf.loc[[f'{int(p*100)}%' for p in trunc_percentiles]]
        pn_cf_truncated = pn_cf.loc[[f'{int(p*100)}%' for p in trunc_percentiles]]
        
        # Plot post_bm percentiles
        line1, = ax.plot(post_bm_cf_truncated.index, post_bm_cf_truncated.values, label='With Balancing Market', color='blue')
        
        # Plot pn percentiles
        line2, = ax.plot(pn_cf_truncated.index, pn_cf_truncated.values, label='Without Balancing Market', color='orange')
        
        ax.set_ylabel(f'{year}', fontsize=9)
        
        # Add annotations for specific years
        if year == 2020:
            ax.annotate('Low demand due to COVID', xy=(0.5, 0.3), xycoords='axes fraction', fontsize=8,
                        bbox=dict(facecolor='white', alpha=0.7))
        elif year == 2022:
            ax.annotate('Ukraine/French-nuke/hydro shock boost', xy=(0.5, 0.5), xycoords='axes fraction', fontsize=8,
                        bbox=dict(facecolor='white', alpha=0.7))
        elif year == 2024:
            ax.annotate('Incomplete data', xy=(0.5, 0.3), xycoords='axes fraction', fontsize=8,
                        bbox=dict(facecolor='white', alpha=0.7))
    
    # Handle any remaining empty subplots in the grid if the number of years is odd
    for j in range(i + 1, rows * cols):
        fig.delaxes(axes[j])
    
    # Add a title for the whole figure
    plt.suptitle("Gas Generators increasingly dependent on Balancing Mechanism", fontsize=14, weight='bold')

    # Create a single global legend outside the plot below the title
    fig.legend([line1, line2], labels=['With Balancing Market', 'Without Balancing Market'], loc='upper center', ncol=2, fontsize=10, bbox_to_anchor=(0.5, 0.92))
    
    # Adjust the space to ensure the title is not cut off
    plt.subplots_adjust(top=0.88, hspace=0.4)

    plt.xlabel('Percentiles', fontsize=10)
    plt.xticks(ticks=np.arange(0, len(trunc_percentiles), step=10), labels=[f'{int(trunc_percentiles[i]*100)}%' for i in range(0, len(trunc_percentiles), 10)])
    plt.tight_layout(rect=[0, 0, 1, 0.88])  # Adjust layout but leave space for the title
    
    # Save the plot with LinkedIn aspect ratio
    plt.savefig("bm_pn_capacity_factors_percentiles_truncated_linkedin.png", dpi=300)

def open_all():
    actual = open_actual()
    pn = open_pn()
    df = combine_series(actual, pn)
    yearly_df = split_calendar_years(df)

    year_cf_tuples = []
    for year, year_series in yearly_df:
        post_bm_cf, pn_cf = capacity_factor_by_year(year, year_series)
        year_cf_tuples.append((year, post_bm_cf, pn_cf))
    
    # Plot faceted graphs for all years
    plot_faceted_capacity_factors(year_cf_tuples)
        
    
if __name__ == '__main__':
    open_all()