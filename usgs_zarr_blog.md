# Cloud-optimized USGS Time Series Data

## Introduction
I think most everyone can agree that the cloud computing is super important and is growing in importance. The scalability, flexibility, and configurabilty of the commercial cloud provide new and exciting possibilities for many applications including earth science research. The use and advantages of the cloud are being fully explored at the US Geological Survey. The full benefit of the cloud, however, can only be realized if the data that will be used from the cloud platform are in a format which allows cloud-based access. Often this means that the data should be in chunked data.

So in this blog post I'll be comparing three formats for storing time series data: Zarr, Parquet, and CSV. Zarr and Parquet are compressed, binary, data formats that can also be chunked or partitioned. This makes them a good choice for use on cloud-based environments where the number of computational cores can be increased a lot. CSV is, of course, a long-time standard format that is a plain-text version of the data.

I am doing this comparision using time series data that are stored in and served from the USGS National Water Information System (NWIS). NWIS serves time series recorded at thousands observation locations throughout the US. These observations are of dozens of water science variables. My comparison will use discharge (or streamflow) data that were, for the most part, recorded 15 minute intervals.

## Benchmarking CSV, Zarr, and Parquet for USGS discharge data
### Initial data gathering
The discharge data were first gathered and stored in an S3 bucket. These data were gathered from more than 12,000 stations across the US for a period of record of 1970-2019. This gathering was done using the NWIS web services ()[] on an AWS EC2 machine. Given preliminary experiments, I wrote all of the data to Zarr as a baseline storage format.

### Zarr vs CSV vs Parquet comparison  
I selected a sub-basin of the Delaware River Basin to do the comparison between the file formats.Recently, the USGS initiated a program for the Next Generation of Water Observation System in the Delaware River Basin. 
