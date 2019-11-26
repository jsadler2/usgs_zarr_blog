# Cloud-optimized USGS Time Series Data

## Introduction
I think most everyone can agree that the cloud computing is super important and is growing in importance. The scalability, flexibility, and configurabilty of the commercial cloud provide new and exciting possibilities for many applications including earth science research. The use and advantages of the cloud are being fully explored at the US Geological Survey. The full benefit of the cloud, however, can only be realized if the data that will be used from the cloud platform are in a format which allows cloud-based access. Often this means that the data should be in chunked data.

So in this blog post I'll be comparing three formats for storing time series data: Zarr, Parquet, and CSV. Zarr ([zarr.readthedocs.io](https://zarr.readthedocs.io/en/stable/)) and Parquet ([parquet.apache.org](https://parquet.apache.org)) are compressed, binary, data formats that can also be chunked or partitioned. This makes them a good choice for use on cloud-based environments where the number of computational cores can be increased a lot. CSV is, of course, a long-time standard format that is a plain-text version of the data.

I am doing this comparision using time series data that are stored in and served from the USGS National Water Information System (NWIS). NWIS serves time series recorded at thousands observation locations throughout the US. These observations are of dozens of water science variables. My comparison will use discharge (or streamflow) data that were, for the most part, recorded 15 minute intervals.

## Comparison set up
There were two major comparisons. The first was to compare the time required to retrieve and format data for a subset of stations from NWIS verses Zarr. The second was to compare the write/read speeds and storage requirements between Zarr, Parquet, and CSV. I did these comparisons using a stock AWS EC2 t3a.large machine (8GB memory). The code I used to do these comparisons is [here](https://github.com/jsadler2/usgs_zarr_blog/blob/master/comparison.py). Both comparisons were done for a 10-day period and then a 40-year period.  

### Initial data gathering
First I gathered the discharge data and wrote them to an S3 bucket. These data were gathered from more than 12,000 stations across the US for a period of record of 1970-2019 using the NWIS web services ([waterservices.usgs.gov/rest/IV-Service.html](https://waterservices.usgs.gov/rest/IV-Service.html)). Given preliminary experiments, I wrote all of the data to Zarr as a baseline storage format.


### Subset of basins
I selected a sub-basin of the Delaware River Basin to do the comparison between the file formats. Recently, the USGS initiated a program for the Next Generation Water Observation System ([NGWOS](https://www.usgs.gov/mission-areas/water-resources/science/usgs-next-generation-water-observing-system-ngwos?qt-science_center_objects=0#qt-science_center_objects)) in the Delaware River Basin. 

Todo: include figure

### Comparison 1: Data Retrieval/Formatting (Zarr vs. NWIS Web services)
I recorded the time it took to retrieve and format data using the NWIS web services and then from Zarr. For the formatting of the data, I converted the data into a Pandas DataFrame with a DateTime index. I did this for one station (the overall outlet) and for all 23 stations in the sub-basin. I intended this comparison to answer the question: "If I have a bunch of sites in a data base (NWIS) or if I have a bunch of sites in a Zarr store, which one performs better at retrieving a relevant subset?" 

### Comparison 2: Data write, read, and storage (Zarr vs. Parquet vs CSV)
Once I retrieved the data subset, I wrote this subset to a new Zarr store, a Parquet file, and a CSV file. All of these files were written on the S3 bucket. I recorded the time it took to write to each of these formats, to read from each, and the storage sizes of each. 

## Results

### Results for 10-day data
Characteristics of pull

|thing|number|
|---|---|
|time period requested (days) | 10|
|sites in all sub-basin | 23|
|time scale (mins) | 15|
|num possible data points | 22,080|
|num data points (removing nan) | 16,905|

#### retrieve/format
| | Zarr | NWIS|
|---|---|---|
|one station (sec)| 6.2 | 1.04| 
|all sub-basin (sec)| 7.2 | 19.7|  

#### read/write/storage
| | Zarr | Parquet| CSV|
|---|---|---| ---|
|write (sec)| 1.23 | 0.15 | 0.15 | 
|read (sec)| 0.64 | 0.16 | 0.17 | 
|storage (kB)| 51.3 | 40.8 | 124.1 | 

### Results for 40-year data
Characteristics of pull

|category|number|
|---|---|
|time period requested (years) | 40|
|sites in all sub-basin | 23|
|time scale (mins) | 15|
|num possible data points | 335,800 |
|num data points (removing nan) | ? |

#### retrieve/format
| | Zarr | NWIS|
|---|---|---|
|one station (sec)| 10.5 | 29.8 | 
|all sub-basin (sec)| 21 | 830 |  
|all sub-basin retrieve (sec)| | 401 |  

#### read/write/storage
| | Zarr | Parquet| CSV|
|---|---|---| ---|
|write (sec)| 5.5 | 1.7 | 28.4 | 
|read (sec)| 11.2 | 0.7 | 3.4 | 
|storage (MB)| 33.5 | 15.4 | 110 | 


## Discussion
### Zarr much faster at retrieval compared to NWIS
In the retrieval/formatting comparison, the largest difference is seen in when pulling the 40 years of data for all 23 stations. Retrieving the data from S3 bucket in Zarr format was more than 40x faster compared to NWIS web services. The EC2 machine in the same region as the S3 bucket with the Zarr dataset. So it may not be a totally fair comparison but it should highlight the benefit of cloud-proximate computing as well as the readily accessible format. 

### Parquet performed best with subset dataset
