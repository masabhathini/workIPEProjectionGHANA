#!/usr/bin/env python
import os, sys, glob
import numpy as np
from datetime import datetime, timedelta
import xarray as xr
import rioxarray
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from io import StringIO


inputdatapath='data/'
########
sdata = xr.open_mfdataset(inputdatapath + 'subset_tas*.nc').drop('height')
sdata.rio.write_crs("epsg:4326", inplace=True)
lonintervals = np.round(np.arange(-5,5,0.1), 1)
latintervals = np.round(np.arange(3,13,0.1), 1)

for ydata in sdata.groupby('time.year'):
    print('Processing Year: ',ydata[0])
    data = ydata[1].interp(lon=lonintervals,lat=latintervals).groupby('time.date').max()
    ####################
    sample = data.tas[0,:,:].to_dataset()
    df = sample.to_dataframe().reset_index()
    geom = [Point(x,y) for x, y in zip(df['lon'], df['lat'])]
    ########################
    #for nn, ntime in enumerate(xdf.time[0:4]):
    for nn, ntime in enumerate(data.date):
      layername = datetime.strptime(str(ntime.date.date.values),'%Y-%m-%d').strftime(inputdatapath + 'Tmax_%Y-%m-%dT%H_%M_%S')
      outjsonfile = datetime.strptime(str(ntime.date.date.values),'%Y-%m-%d').strftime(inputdatapath + 'Tmax_%Y-%m-%dT%H_%M_%S.geojson')
      outgpkgfile = datetime.strptime(str(ntime.date.date.values),'%Y-%m-%d').strftime(inputdatapath + 'Tmax_%Y-%m-%dT%H_%M_%S.gpkg')
      outsqlitefile = datetime.strptime(str(ntime.date.date.values),'%Y-%m-%d').strftime(inputdatapath + 'Tmax_%Y-%m-%dT%H_%M_%S.sqlite')
      #############################
      print('Processing GeoJson: ', ntime, outjsonfile)
      sample = data.tas[nn,:,:].to_dataset()
      gdf = gpd.GeoDataFrame(sample.tas.to_dataframe(), geometry=geom)
      gdf['date'] = gdf['date'].astype(str)
      ## gdf['bbox'] = gdf.to_json(na='null', show_bbox=True)['bbox']
      print(outjsonfile)
      gdf.to_file(outjsonfile , driver="GeoJSON")
      #print(outgpkgfile)
      #gdf.to_file(outgpkgfile , driver="GPKG",layer=layername, OVERWRITE='YES')
      #print(outsqlitefile)
      #gdf.to_file(outsqlitefile , driver="SQLite",layer=layername, spatialite=True, OVERWRITE='YES')
      print('#############################################')


#for file in sorted(glob.glob(inputdatapath + 'subset_tas*.nc')):
#    print('Processing for: ', file)
#    data = xr.open_dataset(file).drop_dims(['bnds'])
#    data.sel(lon=slice(50,110),lat=slice(-10,50)).to_netcdf(inputdatapath + 'subset_'+ file.split('/')[-1])

#        ydata
