import argparse
import numpy as np
import datetime
import pandas as pd
import sys
import os
import xarray as xr
import boto3

scripts_path =  os.path.abspath("../preprocess_ml_usgs/scripts/")
sys.path.insert(0, scripts_path)
import streamflow_data_retrival as st
from utils import convert_df_to_dataset, load_s3_zarr_store, divide_chunks

bucket_name = 'ds-drb-data'
timing_dir = 'timing'



def delete_item_s3(filename, zarr=False):
    s3 = boto3.resource('s3')
    if zarr:
        bucket = s3.Bucket(bucket_name)
        bucket.objects.filter(Prefix=filename).delete()
    else:
        s3.Object(bucket_name, '{timing_dir}/{filename}').delete()
    


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
    return min(times), times


def ds_to_df(ds):
    df = ds.to_dataframe()
    df.reset_index(inplace=True)
    df = df.pivot(index='datetime', columns='site_code', values='streamflow')
    return df


def retrieve_from_nwis(site_codes, start_date, end_date, n_per_chunk=1):
    chunked_list = divide_chunks(site_codes, n_per_chunk)
    df_list = []
    for site_code_chunk in chunked_list:
        d = st.get_streamflow_data(site_code_chunk, start_date, end_date, 'iv',
                                   '15T')
        df_list.append(d)
    df_comb = pd.concat(df_list, 1)
    return df_comb


def load_zarr_discharge():
    my_bucket = f'{bucket_name}/15min_discharge'
    zarr_store = load_s3_zarr_store(my_bucket)
    ds = xr.open_zarr(zarr_store)
    return ds


def get_zarr_data(sites, start_date, end_date):
    """
    get data from a zarr store then read it into a pandas dataframe
    """
    ds = load_zarr_discharge()
    q = ds['streamflow']
    s = q.loc[start_date:end_date, sites]
    return s


def load_zarr_da(sites, start_date, end_date):
    sites_q = get_zarr_data(sites, start_date, end_date)
    sites_q.load()


def get_df_from_zarr(sites, start_date, end_date):
    sites_q = get_zarr_data(sites, start_date, end_date)
    df = ds_to_df(sites_q)
    return df


def get_file_name(tag, ext):
    file_name = f'{tag}.{ext}'
    key = f'{timing_dir}/{file_name}'
    path = f'{bucket_name}/{key}'
    if ext == 'csv' or ext == 'parquet':
        path = "s3://"+path
    return path, key


def write_zarr(df, tag):
    path, key = get_file_name(tag, '')
    delete_item_s3(key + '/', zarr=True)
    ds = convert_df_to_dataset(df, 'site_code', 'datetime', 'streamflow', 
            {'datetime': df.shape[0], 'site_code': df.shape[1]})
    zarr_store = load_s3_zarr_store(get_file_name(tag, '')[0])
    ds.to_zarr(zarr_store)


def write_csv(df, tag):
    path, key = get_file_name(tag, 'csv')
    delete_item_s3(key)
    df.to_csv(path)


def write_parquet(df, tag):
    path, key = get_file_name(tag, 'parquet')
    delete_item_s3(key)
    df.to_parquet(path)


def read_zarr(tag):
    zarr_store = load_s3_zarr_store(get_file_name(tag, '')[0])
    ds = xr.open_zarr(zarr_store)
    q = ds['streamflow'].load()
    return q


def read_csv(tag):
    path, key = get_file_name(tag, 'csv')
    df = pd.read_csv(path, index_col='datetime', parse_dates=['datetime'],
                     infer_datetime_format=True)
    return df


def read_parquet(tag):
    path, key = get_file_name(tag, 'parquet')
    df = pd.read_parquet(path)
    return df


def get_all_drb_sites():
    data_file = 'data/drb_streamflow_sites_table.csv'
    site_code_df = pd.read_csv(data_file, dtype=str)
    site_code_col = 'identifier'
    site_codes = site_code_df[site_code_col].to_list()
    site_codes = [s.replace('USGS-', '') for s in site_codes]
    return site_codes


def get_only_sites_in_zarr(sites):
    ds = load_zarr_discharge()
    sites_arr = np.array(sites)
    sites_in_zarr_mask = np.isin(sites_arr, ds.site_code)
    sites_in_zarr = sites_arr[sites_in_zarr_mask]
    return list(sites_in_zarr)


def get_subset_in_zarr():
    subset_sites = get_subset_sites()
    subset_in_zarr = get_only_sites_in_zarr(subset_sites)
    return subset_in_zarr


def get_all_drb_in_zarr():
    drb_sites = get_all_drb_sites()
    drb_in_zarr = get_only_sites_in_zarr(drb_sites)
    return drb_in_zarr


def get_subset_sites():
    data_file = 'data/nwis_comids-01474500.csv'
    site_cd_col = 'nwis_site_code'
    subset_stations = pd.read_csv(data_file, dtype=str)[site_cd_col].to_list()
    return subset_stations


def time_retrieve_nwis(n_trials, tag):
    # retrieve data for all stations
    # nwis
    sites_tag, start_date, end_date, n_per_chunk = tag.split('_')
    sites = get_sites_from_site_tag(sites_tag)
    out_file = f"results/retrieve_nwis_{tag}.out"

    nwis_one_site = time_function(retrieve_from_nwis, n_trials, ['01474500'],
                                   start_date, end_date)
    nwis_all_sites = time_function(retrieve_from_nwis, n_trials, sites,
                                   start_date, end_date, n_per_chunk)

    with open(out_file, 'w') as f:
        print('nwis all sites time:', nwis_all_sites, file=f)
        print('nwis one site time:', nwis_one_site, file=f)


def time_retrieve_zarr(n_trials, tag):
    sites_tag, start_date, end_date = tag.split('_')
    sites = get_sites_from_site_tag(sites_tag)
    out_file = f"results/retrieve_zarr_{tag}.out"
    zarr_all_sites = time_function(load_zarr_da, n_trials, sites, start_date,
                                   end_date)

    # Zarr
    zarr_one_site = time_function(load_zarr_da, n_trials, ['01474500'],
                                  start_date, end_date)
    with open(out_file, 'w') as f:
        print('zarr all sites time:', zarr_all_sites, file=f)
        print('zarr one site time:', zarr_one_site, file=f)


def time_write(n_trials, tag):
    # get subset from full zarr
    print(tag)
    sites_tag, start_date, end_date = tag.split('_')
    out_file = f"results/write_{tag}.out"
    sites = get_sites_from_site_tag(sites_tag)
    df = get_df_from_zarr(sites, start_date, end_date)

    write_zarr_time = time_function(write_zarr, n_trials, df, tag)
    write_parquet_time = time_function(write_parquet, n_trials, df, tag)
    write_csv_time = time_function(write_csv, n_trials, df, tag)

    with open(out_file, 'w') as f:
        print('write zarr:', write_zarr_time, file=f)
        print('write parquet:', write_parquet_time, file=f)
        print('write csv:', write_csv_time, file=f)


def time_read(n_trials, tag):
    out_file = f"results/read_{tag}.out"
    read_zarr_time = time_function(read_zarr, n_trials, tag)
    read_parquet_time = time_function(read_parquet, n_trials, tag)
    read_csv_time = time_function(read_csv, n_trials, tag)

    with open(out_file, 'w') as f:
        print('read zarr:', read_zarr_time, file=f)
        print('read parquet:', read_parquet_time, file=f)
        print('read csv:', read_csv_time, file=f)


def get_sites_from_site_tag(site_tag):
    """
    get the sites based on the site tag
    :param site_tag:[str] 'md' or 'lg' for medium (Schuylkill) or large (DRB)
    :return:[list] list of sites
    """
    if site_tag == 'md':
        site_codes = get_subset_in_zarr()
    elif site_tag == 'lg':
        site_codes = get_all_drb_in_zarr()
    else:
        raise ValueError('site_tag should be "md" or "lg" not {site_tag}')
    return site_codes


