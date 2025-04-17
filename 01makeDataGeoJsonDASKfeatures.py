#!/usr/bin/env python
import os, sys, glob, dask
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

def createGeoJson(index, date, da):
    sample_ds = da.interp(lon=lonintervals,lat=latintervals).groupby('time.date').max().compute()
    # Convert to dataframe
    df = sample_ds.to_dataframe().reset_index()
    df['geometry'] = [Point(x, y) for x, y in zip(df['lon'], df['lat'])]
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf['date'] = gdf['date'].astype(str)
    # Output filename
    layername = date[0].strftime(inputdatapath + 'daskTmax_%Y-%m-%dT%H_%M_%S')
    outjsonfile = date[0].strftime(inputdatapath + 'daskTmax_%Y-%m-%dT%H_%M_%S.geojson')
    outgpkgfile = date[0].strftime(inputdatapath + 'daskTmax_%Y-%m-%dT%H_%M_%S.gpkg')
    outsqlitefile = date[0].strftime(inputdatapath + 'daskTmax_%Y-%m-%dT%H_%M_%S.sqlite')
    print(f"Saving {outjsonfile}")
    gdf.to_file(outjsonfile, driver="GeoJSON")
    return("Written: ", outjsonfile)


def main_work():
    from dask.distributed import Client, as_completed
    client = Client(n_workers=1, threads_per_worker=64)
    print('Hello client !!!', client)
    futures = list()
    for index, stamp in enumerate(sdata.groupby('time.date')):
        futures.append(client.submit(createGeoJson,index,stamp[0],stamp[1]))
    for future in as_complete(futures):
        print(future.status)

##############
if __name__== '__main__':
    main_work()
