## Results for small pull
pull characteristics

|thing|number|
|---|---|
|time period requested (days) | 10|
|sites in all sub-basin | 23|
|time scale (mins) | 15|
|num possible data points | 22,080|
|num data points (removing nan) | 16,905|

### retrieve
| | Zarr | NWIS|
|---|---|---|
|one station (sec)| 6.2 | 1.04| 
|all sub-basin (sec)| 7.2 | 19.7|  

### read/write/storage
| | Zarr | Parquet| CSV|
|---|---|---| ---|
|write (sec)| 1.23 | 0.15 | 0.15 | 
|read (sec)| 0.64 | 0.16 | 0.17 | 
|storage (kB)| 51.3 | 40.8 | 124.1 | 

## Results for big pull
pull characteristics

|thing|number|
|---|---|
|time period requested (days) | 14600|
|sites in all sub-basin | 23|
|time scale (mins) | 15|
|num possible data points | 335,800 |
|num data points (removing nan) | 16,905|

### retrieve/format
| | Zarr | NWIS|
|---|---|---|
|one station (sec)| 10.5 | 29.8 | 
|all sub-basin (sec)| 886 | 23 |  
|all sub-basin retrieve (sec)| 401 | |  

### read/write/storage
| | Zarr | Parquet| CSV|
|---|---|---| ---|
|write (sec)| 3.6 | 1.7 | 32.5 | 
|read (sec)| 12.1 | 0.2 | 0.2 | 
|storage (MB)| 33.5 | 110.1 | 15.4 | 


