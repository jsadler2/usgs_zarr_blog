import xarray as xr
import s3fs


def load_s3_zarr_store(s3_zarr_path):
    fs = s3fs.S3FileSystem()
    zarr_store = s3fs.S3Map(s3_zarr_path, s3=fs)
    return zarr_store


def convert_df_to_dataset(df, col_name, idx_name, data_name, chunks=None):
    data_array = xr.DataArray(df.values, [(idx_name, df.index),
                                          (col_name, df.columns)])
    data_set = xr.Dataset({data_name: data_array})

    if chunks:
        data_set = data_set.chunk(chunks)
    return data_set


def divide_chunks(l, n):
    """
    divide a single list, l, into sublists of size n
    """
    # looping till length l 
    for i in range(0, len(l), n):
        yield l[i:i + n]
