#!/usr/bin/env python
import os, sys, glob
import pandas as pd
import xarray as xr

inputdatapath='/home/masabas/workIPEProjection/data585/'
#resamplefile = xr.open_dataset(inputdatapath + 'GHANA.nc')
########
#projectiondata = xr.open_mfdataset(inputdatapath + 'tas*.nc')

for file in sorted(glob.glob(inputdatapath + 'tas*.nc')):
    print('Processing for: ', file)
    data = xr.open_dataset(file).drop_dims(['bnds'])
    # Convert the longitude from 0-360 to -180 to 180
    data['lon'] = (((data['lon'] + 360) % 360) - 180)
    data.sel(lon=slice(-15,15),lat=slice(-10,20)).to_netcdf('data/subset_'+ file.split('/')[-1])
    data.close()

#    for ydata in data.groupby('time.year'):
#        ydata
