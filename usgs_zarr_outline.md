# Cloud-optimized USGS Time Series Data

## Introduction
* The cloud is becoming more important
    * data volumes, ML/AI
* The USGS is moving toward leveraging cloud technology
* Cloud-optimized data formats are critical to effectively using the cloud
* What is Zarr
* What is Parquet
* Brief summary of NWIS data

## Benchmarking Zarr for USGS Time Series Data
* Zarr vs CSV vs Parquet
* Introduce stream gauge stations (using a subcatchment of Delaware River Basin)
* Introduce metrics
    * speed for retrieval/writing/analysis
    * storage size for writing
    * ability to capture metadata/structure
* Tests:
    * Data retrieval
        * Retrieve streamflow data for one station
        * Retrieve streamflow data for all stations in subcatchment
    * Data writing
        * Write data
    * Data analysis
        * take monthly average station by station

## Discussion?/Conclusion
* Benefits of Zarr compared to CSV
* Drawbacks of Zarr compared to CSV
* Other cloud data options
* Summary of results
