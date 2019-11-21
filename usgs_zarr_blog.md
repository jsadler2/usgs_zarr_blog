# Cloud-optimized USGS Time Series Data

## Introduction
I think most everyone can agree that the cloud computing is super important and is growing in importance. The scalability, flexibility, and configurabilty of the commercial cloud provide new and exciting possibilities for many applications including earth science research. The use and advantages of the cloud are being fully explored at the US Geological Survey. The full benefit of the cloud, however, can only be realized if the data that will be used from the cloud platform are in a format which allows cloud-based access. Often this means that the data should be in chunked data.

So in this blog post I'll be comparing three formats for storing time series data: Zarr, Parquet, and CSV. Zarr ()[] and Parquet ()[] are compressed, binary, data formats that can also be chunked or partitioned. This makes them a good choice for use on cloud-based environments where the number of computational cores can be increased a lot. CSV is, of course, a long-time standard format that is a plain-text version of the data.

I am doing this comparision using time series data that are stored in and served from the USGS National Water Information System (NWIS). NWIS serves time series recorded at thousands observation locations throughout the US. These observations are of dozens of water science variables. My comparison will use discharge (or streamflow) data that were, for the most part, recorded 15 minute intervals.

## Comparison set up
There were two major comparisons. The first was to compare the time required to subset data from NWIS verses Zarr. The second was to compare the write/read speeds and storage requirements between Zarr, Parquet, and CSV. I did these comparisons using a stock AWS EC2 t3a.large machine (8GB memory). The code I used to do these comparisons is here ()[]. For timing, I used the timeit package in Python to run the operations ? times each to account for inconsistencies in execution time.

### Initial data gathering
First I gathered the discharge data and wrote them to an S3 bucket. These data were gathered from more than 12,000 stations across the US for a period of record of 1970-2019 using the NWIS web services ()[]. Given preliminary experiments, I wrote all of the data to Zarr as a baseline storage format.


### Subset of basins
I selected a sub-basin of the Delaware River Basin to do the comparison between the file formats. Recently, the USGS initiated a program for the Next Generation Water Observation System ()[] in the Delaware River Basin. 

Todo: include figure

### Comparison 1: Data Retrieval/Formatting (Zarr vs. NWIS Web services)
I recorded the time it takes to retrieve and format data using the NWIS web services and then from Zarr. For the formatting of the data, I needed the data in a Pandas DataFrame with a DateTime index. I did this for one station (the overall outlet) and the same thing for all of the stations in the sub-basin. I intended this comparison to answer the question: "If I have a bunch of sites in a data base (NWIS) or if I have a bunch of sites in a Zarr store, which one is faster to retrieve a relevant subset?" 

pseudo code: 

### Comparison 2: Data write, read, and storage (Zarr vs. Parquet vs CSV)
Once I retrieved the data subset, I wrote this subset to a new Zarr store, a Parquet file, and a CSV file. I recorded the time it took to write to each of these formats, to read from each, and the storage sizes of each. 

### Results

## Discussion
The EC2 machine in the same region as the S3 bucket with the Zarr dataset. So it may not be a totally fair comparison but it should highlight the benefit of cloud-proximate computing as well as the readily accessible format. 
