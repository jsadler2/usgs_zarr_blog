import datetime
import pandas as pd
import sys
import os
import xarray as xr

scripts_path =  os.path.abspath("../preprocess_ml_usgs/scripts/")
sys.path.insert(0, scripts_path)
import streamflow_data_retrival as st

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


def get_zarr_data(sites):
    """
    get and persist data from a zarr store then read it into a pandas dataframe
    """
    fs = s3fs.S3FileSystem()
    my_bucket = 'ds-drb-data/'
    file_name = f'{my_bucket}15min_discharge'
    zarr_store = s3fs.S3Map(file_name, s3=fs)
    ds = xr.open_zarr(zarr_store)
    q = ds['streamflow']
    s = q[sites]
    df = s.to_dataframe()
    return df


# read in all stations
data_file = 'data/nwis_comids-01474500.csv'
site_code_col = 'nwis_site_code'
subset_stations = pd.read_csv(data_file)[site_code_col]
outlet_id = '01474500'
start_date = '1970-01-01'
end_date = '2019-01-10'
n_trials = 10

# retrieve data for just the outlet
# NWIS
sites = [outlet_id]
nwis_one_site = time_function(st.get_streamflow_data, n_trials, sites,
                              start_date, end_date, 'iv', '15T')
print('nwis one site time:', nwis_one_site)
# Zarr
zarr_one_site = time_function(get_zarr_data, n_trials, sites)
print('zarr one site time:', zarr_one_site)


# retrieve data for all stations
# NWIS
sites = subset_stations
nwis_all_sites = time_function(st.get_streamflow_data, 10, sites, start_date,
                               end_date, 'iv', '15T')
print('nwis all sites time:', nwis_all_sites)
# Zarr
zarr_all_sites = time_function(get_zarr_data, n_trials, sites)
print('zarr all sites time:', zarr_all_sites)

