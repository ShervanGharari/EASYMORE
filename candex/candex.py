# section 1 load all the necessary modules and packages
import glob
import time
import geopandas as gpd
import netCDF4 as nc4
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon
import matplotlib.pyplot as plt
import shapefile # pyshed library
import sys
import os
from datetime import datetime
from simpledbf import Dbf5



def NetCDF_SHP_lat_lon(name_of_nc, name_of_variable, name_of_lat_var, name_of_lon_var, name_of_shp, box_values, correct_360):
    """
    @ author:                  Shervan Gharari
    @ Github:                  https://github.com/ShervanGharari/candex
    @ author's email id:       sh.gharari@gmail.com
    @license:                  Apache2

    This function gets a NetCDF file the assosiated shapefile given the cordination of a given box
    if correct_360 is True then the code convert the lon values more than 180 to negative lon

    Arguments
    ---------
    name_of_nc: string, the name of the nc file
    name_of_variable: string, the name of [sample] variable from nc file
    name_of_lat_var: string, the name of the variable lat
    name_of_lon_var: string, the name of the variable lon
    name_of_shp: string, the name of the shapfile to be created
    box_values: the box to limit to a specific domain or boolean of False
    correct_360: logical, True or Flase
    Returns
    -------
    """
    # open the nc file to read
    ncid = nc4.Dataset(name_of_nc)

    # deciding which case
    # case #1 regular lat/lon
    if (len(ncid.variables[name_of_lat_var].dimensions)==1) and (len(ncid.variables[name_of_lon_var].dimensions)==1) and\
       (len(ncid.variables[name_of_variable].dimensions)==3):
        print('case 1 - regular lat/lon')
        # get the list of dimensions for the ncid sample varibale
        list_dim_name = list(ncid.variables[name_of_variable].dimensions)
        # get the location of lat dimensions
        location_of_lat = list_dim_name.index(list(ncid.variables[name_of_lat_var].dimensions)[0])
        locaiton_of_lon = list_dim_name.index(list(ncid.variables[name_of_lon_var].dimensions)[0])
        # det the dimensions of lat and lon
        len_of_lat = len(ncid.variables[name_of_lat_var][:])
        len_of_lon = len(ncid.variables[name_of_lon_var][:])
        print(len_of_lat, len_of_lon)
        if locaiton_of_lon > location_of_lat:
            lat = np.zeros([len_of_lat, len_of_lon])
            lon = np.zeros([len_of_lat, len_of_lon])
            for i in np.arange(len(ncid.variables[name_of_lon_var][:])):
                lat [:,i] = ncid.variables[name_of_lat_var][:]
            for i in np.arange(len(ncid.variables[name_of_lat_var][:])):
                lon [i,:] = ncid.variables[name_of_lon_var][:]
        else:
            lat = np.zeros([len_of_lon, len_of_lat])
            lon = np.zeros([len_of_lon, len_of_lat])
            for i in np.arange(len(ncid.variables[name_of_lon_var][:])):
                lat [i,:] = ncid.variables[name_of_lat_var][:]
            for i in np.arange(len(ncid.variables[name_of_lat_var][:])):
                lon [:,i] = ncid.variables[name_of_lon_var][:]
    # case #2 rotated lat/lon
    if (len(ncid.variables[name_of_lat_var].dimensions)==2) and (len(ncid.variables[name_of_lon_var].dimensions)==2):
        print('case 2 - rotated lat/lon')
        lat = ncid.variables[name_of_lat_var][:,:]
        lon = ncid.variables[name_of_lon_var][:,:]
    # case #3 1-D lat/lon and 2 data for irregulat shapes
    if (len(ncid.variables[name_of_lat_var].dimensions)==1) and (len(ncid.variables[name_of_lon_var].dimensions)==1) and\
       (len(ncid.variables[name_of_variable].dimensions)==2):
        print('case 3 - regular lat/lon; shapefile should be provided')
        sys.exit("no shapfile is created, please provide the associated shapefile to the netCDF file")

    # creating/saving the shapefile
    lat_lon_SHP(name_of_shp, lat, lon, box_values, correct_360)

    # return mapped lat lon (2D lat, lon)
    return lat, lon


def lat_lon_SHP(name_of_shp, lat, lon, box_values, correct_360):
    """
    @ author:                  Shervan Gharari, Wouter Knoben
    @ Github:                  https://github.com/ShervanGharari/candex
    @ author's email id:       sh.gharari@gmail.com
    @ license:                 Apache2

    This function gets a 2-D lat and lon and return the shapefile given the lat and lon matrices
    The function return a shapefile within the box_values specify by the model simulation.
    correct_360 is True, then the values of more than 180 for the lon are converted to negative lon
    correct_360 is False, then the cordinates of the shapefile remain in 0 to 360 degree
    The function remove the first, last rows and colomns

    Arguments
    ---------
    lat: the 2D matrix of lat_2D [n,m,]
    lon: the 2D matrix of lon_2D [n,m,]
    box_values: a 1D array [minlat, maxlat, minlon, maxlon]
    correct_360: logical, True or Flase

    Returns
    -------
    result: a shapefile with (n-2)*(m-2) elements depicting the provided 2-D lat and lon values
    """
    # check if lat/lon that are taken in has the same dimnesion
    if ((lat.shape[0] != lon.shape[0]) or (lat.shape[1] != lon.shape[1])):
        sys.exit("no shapfile is created, please provide the associated shapefile to the netCDF file")

    # check for the shapefile covers two hemisphere
    # to be done
    #
    if type(box_values) == bool:
        if not box_values:
            box_values = np.array([0,0,0,0])
            box_values[0] = -10**6
            box_values[1] = +10**6
            box_values[2] = -10**6
            box_values[3] = +10**6
        else:
            sys.exit('box_values should be either False or a numpy array specifying numpy.array[min_lat, max_lat, min_lon, max_lon]')

    # check if there are at the two size
    if correct_360 is True:
        print("Warning: use the bounding box to focuse on the regiong of study, avoid region close to 0 or 360")
        print("Warning: it is suggested to make your target shapefile from -180-180 to 0-360")
        idx = lon>180 # index of more than 180
        lon[idx] = lon[idx]-360 # index of point with higher than are reduced to -180 to 0 instead

    # get the shape
    lat_lon_shape = lat.shape

    # write the shapefile
    with shapefile.Writer(name_of_shp) as w:
        w.autoBalance = 1 # turn on function that keeps file stable if number of shapes and records don't line up
        w.field("ID_s",'N') # create (N)umerical attribute fields, integer
        w.field("lat_s",'F',decimal=4) # float with 4 decimals
        w.field("lon_s",'F',decimal=4) # float with 4 decimals

        # preparing the m whcih is a couter for the shapefile arbitrary ID
        m = 0.00

        # itterating to create the shapes of the result shapefile ignoring the first and last rows and columns
        for i in range(1, lat_lon_shape[0] - 1):
            for j in range(1, lat_lon_shape[1] - 1):
                # checking is lat and lon is located inside the provided box
                if  lat[i, j] > box_values[0] and lat[i, j] < box_values[1] and\
                    lon[i, j] > box_values[2] and lon[i, j] < box_values[3]:

                    # empty the polygon variable
                    parts = []

                    # update records
                    m += 1 # ID
                    center_lat = lat[i,j] # lat value of data point in source .nc file
                    if correct_360:
                        center_lon = lon[i,j] + 360 # lon value of data point in source .nc file should be within [0,360]
                    else:
                        center_lon = lon[i,j]      # lon vaue of data point in source .nc file is within [-180,180]

                    # Creating the lat of the shapefile
                    Lat_Up       = (lat[i - 1, j] + lat[i, j]) / 2
                    Lat_UpRright = (lat[i - 1, j] + lat[i - 1, j + 1] + lat[i, j + 1] + lat[i, j]) / 4
                    Lat_Right    = (lat[i, j + 1] + lat[i, j]) / 2
                    Lat_LowRight = (lat[i, j + 1] + lat[i + 1, j + 1] + lat[i + 1, j] + lat[i, j]) / 4
                    Lat_Low      = (lat[i + 1, j] + lat[i, j]) / 2
                    Lat_LowLeft  = (lat[i, j - 1] + lat[i + 1, j - 1] + lat[i + 1, j] + lat[i, j]) / 4
                    Lat_Left     = (lat[i, j - 1] + lat[i, j]) / 2
                    Lat_UpLeft   = (lat[i - 1, j - 1] + lat[i - 1, j] + lat[i, j - 1] + lat[i, j]) / 4

                    # Creating the lon of the shapefile
                    Lon_Up       = (lon[i - 1, j] + lon[i, j]) / 2
                    Lon_UpRright = (lon[i - 1, j] + lon[i - 1, j + 1] + lon[i, j + 1] + lon[i, j]) / 4
                    Lon_Right    = (lon[i, j + 1] + lon[i, j]) / 2
                    Lon_LowRight = (lon[i, j + 1] + lon[i + 1, j + 1] + lon[i + 1, j] + lon[i, j]) / 4
                    Lon_Low      = (lon[i + 1, j] + lon[i, j]) / 2
                    Lon_LowLeft  = (lon[i, j - 1] + lon[i + 1, j - 1] + lon[i + 1, j] + lon[i, j]) / 4
                    Lon_Left     = (lon[i, j - 1] + lon[i, j]) / 2
                    Lon_UpLeft   = (lon[i - 1, j - 1] + lon[i - 1, j] + lon[i, j - 1] + lon[i, j]) / 4

                    # creating the polygon given the lat and lon
                    parts.append([ (Lon_Up,        Lat_Up),\
                                   (Lon_UpRright,  Lat_UpRright), \
                                   (Lon_Right,     Lat_Right), \
                                   (Lon_LowRight,  Lat_LowRight), \
                                   (Lon_Low,       Lat_Low), \
                                   (Lon_LowLeft,   Lat_LowLeft), \
                                   (Lon_Left,      Lat_Left), \
                                   (Lon_UpLeft,    Lat_UpLeft), \
                                   (Lon_Up,        Lat_Up)])

                    # store polygon
                    w.poly(parts)

                    # update records/fields for the polygon
                    w.record(m, center_lat, center_lon)
    return

def intersection_shp(shp_1, shp_2):
    """
    @ author:                  Shervan Gharari
    @ Github:                  https://github.com/ShervanGharari/candex
    @ author's email id:       sh.gharari@gmail.com
    @license:                  Apache2
    This fucntion intersect two shapefile. It keeps the fiels from the first and second shapefiles (identified by S_1_ and
    S_2_). It also creats other field including AS1 (area of the shape element from shapefile 1), IDS1 (an arbitary index
    for the shapefile 1), AS2 (area of the shape element from shapefile 1), IDS2 (an arbitary index for the shapefile 1),
    AINT (the area of teh intersected shapes), AP1 (the area of the intersected shape to the shapes from shapefile 1),
    AP2 (the area of teh intersected shape to the shapefes from shapefile 2), AP1N (the area normalized in the case AP1
    summation is not 1 for a given shape from shapefile 1, this will help to preseve mass if part of the shapefile are not
    intersected), AP2N (the area normalized in the case AP2 summation is not 1 for a given shape from shapefile 2, this
    will help to preseve mass if part of the shapefile are not intersected)

    Arguments
    ---------
    shp1: geo data frame, shapefile 1
    shp2: geo data frame, shapefile 2

    Returns
    -------
    result: a geo data frame that includes the intersected shapefile and area, percent and normalized percent of each shape
    elements in another one
    """
    # Calculating the area of every shapefile (both should be in degree or meters)
    column_names = shp_1.columns
    column_names = list(column_names)

    # removing the geometry from the column names
    column_names.remove('geometry')

    # renaming the column with S_1
    for i in range(len(column_names)):
        shp_1 = shp_1.rename(
            columns={column_names[i]: 'S_1_' + column_names[i]})

    column_names = shp_2.columns
    column_names = list(column_names)

    # removing the geometry from the colomn names
    column_names.remove('geometry')

    # renaming the column with S_2
    for i in range(len(column_names)):
        shp_2 = shp_2.rename(
            columns={column_names[i]: 'S_2_' + column_names[i]})

    # Caclulating the area for shp1
    shp_1['AS1'] = shp_1.area
    shp_1['IDS1'] = np.arange(shp_1.shape[0])+1

    # Caclulating the area for shp2
    shp_2['AS2'] = shp_2.area
    shp_2['IDS2'] = np.arange(shp_2.shape[0])+1

    # making intesection
    result = spatial_overlays (shp_1, shp_2, how='intersection')

    # Caclulating the area for shp2
    result['AINT'] = result['geometry'].area
    result['AP1'] = result['AINT']/result['AS1']
    result['AP2'] = result['AINT']/result['AS2']


    # taking the part of data frame as the numpy to incread the spead
    # finding the IDs from shapefile one
    ID_S1 = np.array (result['IDS1'])
    AP1 = np.array(result['AP1'])
    AP1N = AP1 # creating the nnormalized percent area
    ID_S1_unique = np.unique(ID_S1) #unique idea
    for i in ID_S1_unique:
        INDX = np.where(ID_S1==i) # getting the indeces
        AP1N[INDX] = AP1[INDX] / AP1[INDX].sum() # normalizing for that sum

    # taking the part of data frame as the numpy to incread the spead
    # finding the IDs from shapefile one
    ID_S2 = np.array (result['IDS2'])
    AP2 = np.array(result['AP2'])
    AP2N = AP2 # creating the nnormalized percent area
    ID_S2_unique = np.unique(ID_S2) #unique idea
    for i in ID_S2_unique:
        INDX = np.where(ID_S2==i) # getting the indeces
        AP2N[INDX] = AP2[INDX] / AP2[INDX].sum() # normalizing for that sum

    result ['AP1N'] = AP1N
    result ['AP2N'] = AP2N

    return result


def spatial_overlays(df1, df2, how='intersection', reproject=True):
    """Perform spatial overlay between two polygons.
    Currently only supports data GeoDataFrames with polygons.
    Implements several methods that are all effectively subsets of
    the union.

    Omer Ozak
    ozak
    https://github.com/ozak
    https://github.com/geopandas/geopandas/pull/338
    Parameters
    ----------
    df1 : GeoDataFrame with MultiPolygon or Polygon geometry column
    df2 : GeoDataFrame with MultiPolygon or Polygon geometry column
    how : string
        Method of spatial overlay: 'intersection', 'union',
        'identity', 'symmetric_difference' or 'difference'.
    use_sindex : boolean, default True
        Use the spatial index to speed up operation if available.
    Returns
    -------
    df : GeoDataFrame
        GeoDataFrame with new set of polygons and attributes
        resulting from the overlay
    """
    df1 = df1.copy()
    df2 = df2.copy()
    df1['geometry'] = df1.geometry.buffer(0)
    df2['geometry'] = df2.geometry.buffer(0)
    if df1.crs!=df2.crs and reproject:
        print('Data has different projections.')
        print('Converted data to projection of first GeoPandas DatFrame')
        df2.to_crs(crs=df1.crs, inplace=True)
    if how=='intersection':
        # Spatial Index to create intersections
        spatial_index = df2.sindex
        df1['bbox'] = df1.geometry.apply(lambda x: x.bounds)
        df1['sidx']=df1.bbox.apply(lambda x:list(spatial_index.intersection(x)))
        pairs = df1['sidx'].to_dict()
        nei = []
        for i,j in pairs.items():
            for k in j:
                nei.append([i,k])
        pairs = gpd.GeoDataFrame(nei, columns=['idx1','idx2'], crs=df1.crs)
        pairs = pairs.merge(df1, left_on='idx1', right_index=True)
        pairs = pairs.merge(df2, left_on='idx2', right_index=True, suffixes=['_1','_2'])
        pairs['Intersection'] = pairs.apply(lambda x: (x['geometry_1'].intersection(x['geometry_2'])).buffer(0), axis=1)
        pairs = gpd.GeoDataFrame(pairs, columns=pairs.columns, crs=df1.crs)
        cols = pairs.columns.tolist()
        cols.remove('geometry_1')
        cols.remove('geometry_2')
        cols.remove('sidx')
        cols.remove('bbox')
        cols.remove('Intersection')
        dfinter = pairs[cols+['Intersection']].copy()
        dfinter.rename(columns={'Intersection':'geometry'}, inplace=True)
        dfinter = gpd.GeoDataFrame(dfinter, columns=dfinter.columns, crs=pairs.crs)
        dfinter = dfinter.loc[dfinter.geometry.is_empty==False]
        dfinter.drop(['idx1','idx2'], inplace=True, axis=1)
        return dfinter
    elif how=='difference':
        spatial_index = df2.sindex
        df1['bbox'] = df1.geometry.apply(lambda x: x.bounds)
        df1['sidx']=df1.bbox.apply(lambda x:list(spatial_index.intersection(x)))
        df1['new_g'] = df1.apply(lambda x: reduce(lambda x, y: x.difference(y).buffer(0),
                                 [x.geometry]+list(df2.iloc[x.sidx].geometry)) , axis=1)
        df1.geometry = df1.new_g
        df1 = df1.loc[df1.geometry.is_empty==False].copy()
        df1.drop(['bbox', 'sidx', 'new_g'], axis=1, inplace=True)
        return df1
    elif how=='symmetric_difference':
        df1['idx1'] = df1.index.tolist()
        df2['idx2'] = df2.index.tolist()
        df1['idx2'] = np.nan
        df2['idx1'] = np.nan
        dfsym = df1.merge(df2, on=['idx1','idx2'], how='outer', suffixes=['_1','_2'])
        dfsym['geometry'] = dfsym.geometry_1
        dfsym.loc[dfsym.geometry_2.isnull()==False, 'geometry'] = dfsym.loc[dfsym.geometry_2.isnull()==False, 'geometry_2']
        dfsym.drop(['geometry_1', 'geometry_2'], axis=1, inplace=True)
        dfsym = gpd.GeoDataFrame(dfsym, columns=dfsym.columns, crs=df1.crs)
        spatial_index = dfsym.sindex
        dfsym['bbox'] = dfsym.geometry.apply(lambda x: x.bounds)
        dfsym['sidx'] = dfsym.bbox.apply(lambda x:list(spatial_index.intersection(x)))
        dfsym['idx'] = dfsym.index.values
        dfsym.apply(lambda x: x.sidx.remove(x.idx), axis=1)
        dfsym['new_g'] = dfsym.apply(lambda x: reduce(lambda x, y: x.difference(y).buffer(0),
                         [x.geometry]+list(dfsym.iloc[x.sidx].geometry)) , axis=1)
        dfsym.geometry = dfsym.new_g
        dfsym = dfsym.loc[dfsym.geometry.is_empty==False].copy()
        dfsym.drop(['bbox', 'sidx', 'idx', 'idx1','idx2', 'new_g'], axis=1, inplace=True)
        return dfsym
    elif how=='union':
        dfinter = spatial_overlays(df1, df2, how='intersection')
        dfsym = spatial_overlays(df1, df2, how='symmetric_difference')
        dfunion = dfinter.append(dfsym)
        dfunion.reset_index(inplace=True, drop=True)
        return dfunion
    elif how=='identity':
        dfunion = spatial_overlays(df1, df2, how='union')
        cols1 = df1.columns.tolist()
        cols2 = df2.columns.tolist()
        cols1.remove('geometry')
        cols2.remove('geometry')
        cols2 = set(cols2).intersection(set(cols1))
        cols1 = list(set(cols1).difference(set(cols2)))
        cols2 = [col+'_1' for col in cols2]
        dfunion = dfunion[(dfunion[cols1+cols2].isnull()==False).values]
        return dfunion

def lat_lon_to_index(lat_source,
                     lon_source,
                     lat_mapped,
                     lon_mapped):
    """
    @ author:                  Shervan Gharari
    @ Github:                  https://github.com/ShervanGharari/candex
    @ author's email id:       sh.gharari@gmail.com
    @license:                  Apache2

    This function get the mapped lat and lon from data created by mapped_lat_lon function and find
    the index for the remap dataframe to that

    Arguments
    ---------
    lat_source: 1D numpy array of lat_source from remap data frame
    lon_source: 1D numpy array of lon_source from remap data frame
    lat_mapped: 2D or 1D numpy array of data lat values
    lon_mapped: 2D or 1D numpy array of data lon values

    Returns
    -------
    rows: a 1D numpy array indexing the row of data to be read
    cols: a 1D numpy array indexing the col of data to be read
    """

    # create the np arrays
    rows = np.zeros(len(lat_source))
    cols = np.zeros(len(lon_source))

    # loop to find the rows and colomns
    for i in np.arange(len(lat_source)):
        lat_lon_value_diff = abs(lat_mapped - lat_source[i])+abs(lon_mapped - lon_source[i])
        row, col = np.where(lat_lon_value_diff == lat_lon_value_diff.min())
        rows [i] = row[0]
        cols [i] = col[0]

    return rows, cols


def weighted_average(nc_name,
                     target_time,
                     dim_time,
                     varibale_name,
                     mapping_df,
                     number_of_target_elements):
    """
    @ author:                  Shervan Gharari
    @ Github:                  https://github.com/ShervanGharari/candex
    @ author's email id:       sh.gharari@gmail.com
    @license:                  Apache2

    This function funcitons read different grids and sum them up based on the
    weight provided to aggregate them over a larger area

    Arguments
    ---------
    nc_name: str, the name of the nc file including its path
    target_time: str, the target time to be read
    dim_time: dimension of time
    varibale_name: name of the varibale to be read from the nc file
    mapping_df: the dataframe used in reading the data including the following colomns:
                ID_target
                lon_target
                lat_target
                ID_source
                lat_source
                lon_source
                rows
                cols
                order

    Returns
    -------
    weighted_value: a numpy array that has the remapped values from the nc file
    """


    # read from mapping data frame and pass to int data array
    rows = np.array(mapping_df['rows'])
    cols = np.array(mapping_df['cols'])
    rows = rows.astype(int) # make sure the indices are int
    cols = cols.astype(int) # make sure the indices are int

    # open dataset
    ds = xr.open_dataset(nc_name)

    # prepared the numpy array for ouptut
    weighted_value = np.zeros([len(target_time),number_of_target_elements])
    m = 0 # couter

    for date in target_time: # loop over time

        ds_temp = ds.sel(time=date.strftime("%Y-%m-%d %H:%M:%S"),method="nearest")
        data = np.array(ds_temp[varibale_name])

        # get values from the rows and cols and pass to np data array
        values = data [rows,cols]
        values = np.array(values)

        # add values to data frame, weighted average and pass to data frame again
        mapping_df['values'] = values
        mapping_df['values_w'] = mapping_df['weight']*mapping_df['values']
        df_temp = mapping_df.groupby(['ID_t'],as_index=False).agg({'values_w': 'sum'})
        df_temp = df_temp.sort_values(by=['ID_t'])
        weighted_value [m,:] = np.array(df_temp['values_w'])

        m = m+1

    return weighted_value


def target_nc_creation(nc_names,
                       remap,
                       name_of_var_time,
                       output_path,
                       varibale_name_list,
                       format_list,
                       fill_value_list,
                       authour_name):
    """
    @ author:                  Shervan Gharari
    @ Github:                  https://github.com/ShervanGharari/candex
    @ author's email id:       sh.gharari@gmail.com
    @license:                  Apache2

    This function funcitons read different grids and sum them up based on the
    weight provided to aggregate them over a larger area

    Arguments
    ---------
    nc_name: str, the name of the nc file including its path
    target_time: str, the target time to be read
    dim_time: dimension of time
    varibale_name: name of the varibale to be read from the nc file
    mapping_df: the dataframe used in reading the data including the following colomns:
                ID_target
                lon_target
                lat_target
                ID_source
                lat_source
                lon_source
                rows
                cols
                order

    Returns
    -------
    weighted_value: a numpy array that has the remapped values from the nc file
    """

    # creating the target_ID_lat_lon
    target_ID_lat_lon = pd.DataFrame()
    target_ID_lat_lon ['ID_t']  = remap ['ID_t']
    target_ID_lat_lon ['lat_t'] = remap ['lat_t']
    target_ID_lat_lon ['lon_t'] = remap ['lon_t']
    target_ID_lat_lon = target_ID_lat_lon.drop_duplicates()
    target_ID_lat_lon = target_ID_lat_lon.sort_values(by=['ID_t'])

    # prepare the hru_id (here COMID), lat, lon
    hruID_var = np.array(target_ID_lat_lon['ID_t'])
    hruID_lat = np.array(target_ID_lat_lon['lat_t'])
    hruID_lon = np.array(target_ID_lat_lon['lon_t'])

    #
    nc_names = glob.glob(nc_names)
    nc_names = sorted(nc_names)

    for nc_name in nc_names:

        # get the time unit and time var from source
        ncids = nc4.Dataset(nc_name)
        if 'units' in ncids.variables[name_of_var_time].ncattrs():
            time_unit = ncids.variables[name_of_var_time].units
        if 'calendar' in ncids.variables[name_of_var_time].ncattrs():
            time_cal = ncids.variables[name_of_var_time].calendar
        time_var = ncids[name_of_var_time][:]
        target_date_times = nc4.num2date(time_var,units = time_unit,calendar = time_cal)
        target_name = output_path+target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.nc'
        if os.path.exists(target_name): # remove file if exists
            os.remove(target_name)

        # reporting
        print('Remapping '+nc_name+' to '+target_name)
        print('Started at date and time '+str(datetime.now()))

        with nc4.Dataset(target_name, "w", format="NETCDF4") as ncid: # creating the NetCDF file

            # define the dimensions
            dimid_N = ncid.createDimension('ID', len(hruID_var))  # limited dimensiton equal the number of hruID
            dimid_T = ncid.createDimension('time', None)   # limited dimensiton, 17544 hr (2 years of 1980 and 1981)

            # Variable time
            time_varid = ncid.createVariable('time', 'i4', ('time', ))
            # Attributes
            time_varid.long_name = name_of_var_time
            time_varid.units = time_unit  # e.g. 'days since 2000-01-01 00:00' should change accordingly
            time_varid.calendar = time_cal
            time_varid.standard_name = name_of_var_time
            time_varid.axis = 'T'
            time_varid[:] = time_var

            # Variables lat, lon, subbasin_ID
            lat_varid = ncid.createVariable('latitude', 'f8', ('ID', ))
            lon_varid = ncid.createVariable('longitude', 'f8', ('ID', ))
            hruId_varid = ncid.createVariable('ID', 'f8', ('ID', ))
            # Attributes
            lat_varid.long_name = 'latitude'
            lon_varid.long_name = 'longitude'
            hruId_varid.long_name = 'subbasin ID'
            lat_varid.units = 'degrees_north'
            lon_varid.units = 'degrees_east'
            hruId_varid.units = '1'
            lat_varid.standard_name = 'latitude'
            lon_varid.standard_name = 'longitude'
            lat_varid[:] = hruID_lat
            lon_varid[:] = hruID_lon
            hruId_varid[:] = hruID_var


            # general attributes for NetCDF file
            ncid.Conventions = 'CF-1.6'
            ncid.License = 'The data were written by ' + authour_name
            ncid.history = 'Created ' + time.ctime(time.time())
            ncid.source = 'Written by script from library of Shervan Gharari (https://github.com/ShervanGharari/candex).'


            # write varibales
            for i in np.arange(len(varibale_name_list)):

                var_value  = weighted_average(nc_name,
                                            target_date_times,
                                            name_of_var_time,
                                            varibale_name_list[i],
                                            remap,
                                            len(hruID_var))

                # Variables tp, total precipitation
                varid = ncid.createVariable(varibale_name_list[i], format_list[i], ('time','ID' ), fill_value = fill_value_list[i])
                varid [:] = var_value
                # Attributes
                if 'long_name' in ncids.variables[varibale_name_list[i]].ncattrs():
                    varid.long_name = ncids.variables[varibale_name_list[i]].long_name
                if 'units' in ncids.variables[varibale_name_list[i]].ncattrs():
                    varid.units = ncids.variables[varibale_name_list[i]].units

            # reporting
            print('Ended   at date and time '+str(datetime.now()))
