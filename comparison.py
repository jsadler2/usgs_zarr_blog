import datetime
import pandas as pd
import sys
import os
import xarray as xr

scripts_path =  os.path.abspath("../preprocess_ml_usgs/scripts/")
sys.path.insert(0, scripts_path)
import streamflow_data_retrival as st
from utils import convert_df_to_dataset, load_s3_zarr_store

def time_function(function, n_loop, *args):
    """
    time an arbitrary function, running it a number of times and returning the
    minimum elapsed time from the number of trials
    :param function: [function] the function that you want to time
    :param n_loop: [int] the number of times to run the function
    :param *args: arguments that will forwarded into the function that will be
    timed
    """
    times = []
    for i in range(n_loop):
        start_time = datetime.datetime.now()
        function(*args)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        times.append(elapsed_time.total_seconds())
    return min(times)


def get_zarr_data(sites, start_date, end_date):
    """
    get and persist data from a zarr store then read it into a pandas dataframe
    """
    my_bucket = 'ds-drb-data/15min_discharge'
    zarr_store = load_s3_zarr_store(my_bucket)
    ds = xr.open_zarr(zarr_store)
    q = ds['streamflow']
    s = q[sites, start_date:end_date]
    df = s.to_dataframe()
    return df


def write_zarr(df):
    ds = convert_df_to_dataset(df, 'site_codes', 'datetime', 'streamflow', 
            {'datetime': df.shape[0], 'site_codes': df.shape[1]})
    zarr_store = load_s3_zarr_store('ds-drb-data/timing_test_zarr')
    ds.to_zarr(zarr_store)


def write_csv(df):
    path = 's3://ds-drb-data/timing_test_csv'
    df.to_csv(path)


def write_parquet(df):
    path = 's3://ds-drb-data/timing_test_parquet'
    df.to_parquet(path)


def read_zarr(df):
    zarr_store = load_s3_zarr_store('ds-drb-data/timing_test_zarr')
    ds = xr.open_zarr(zarr_store)
    q = ds['streamflow']
    df = q.to_dataframe()


def read_csv(df):
    path = 's3://ds-drb-data/timing_test_csv'
    df.read_csv(path)


def read_parquet(df):
    path = 's3://ds-drb-data/timing_test_parquet'
    df.read_parquet(path)

if __name__ == "__main__":
    # SETUP
    # read in all stations
    data_file = 'data/nwis_comids-01474500.csv'
    site_code_col = 'nwis_site_code'
    subset_stations = pd.read_csv(data_file)[site_code_col]
    outlet_id = '01474500'
    start_date = '1970-01-01'
    end_date = '2019-01-10'
    n_trials = 10

    # RETRIEVAL
    # retrieve data for just the outlet
    # nwis
    sites = [outlet_id]
    nwis_one_site = time_function(st.get_streamflow_data, n_trials, sites,
                                  start_date, end_date, 'iv', '15T')
    print('nwis one site time:', nwis_one_site)
    # Zarr
    zarr_one_site = time_function(get_zarr_data, n_trials, sites, start_date,
                                  end_date)
    print('zarr one site time:', zarr_one_site)


    # retrieve data for all stations
    # nwis
    sites = subset_stations
    nwis_all_sites = time_function(st.get_streamflow_data, 10, sites, start_date,
                                   end_date, 'iv', '15T')
    print('nwis all sites time:', nwis_all_sites)
    # Zarr
    zarr_all_sites = time_function(get_zarr_data, n_trials, sites, start_date,
                                   end_date)
    print('zarr all sites time:', zarr_all_sites)

    # WRITE
    # get subset from full zarr
    df = get_zarr_data(sites, start_date, end_date)

    write_zarr_time = time_function(write_zarr, n_trials, df)
    print('write zarr:', write_zarr_time)

    write_parquet_time = time_function(write_parquet, n_trials, df)
    print('write parquet:', write_parquet_time)

    write_csv_time = time_function(write_csv, n_trials, df)
    print('write csv:', write_csv_time)

    # READ
    read_zarr_time = time_function(read_zarr, n_trials, df)
    print('read zarr:', write_zarr_time)

    read_parquet_time = time_function(write_parquet, n_trials, df)
    print('read parquet:', write_parquet_time)

    read_csv_time = time_function(write_csv, n_trials, df)
    print('read csv:', write_csv_time)

