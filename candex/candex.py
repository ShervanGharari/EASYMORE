# section 1 load all the necessary modules and packages

import glob
import time
import netCDF4      as nc4
import numpy        as np
import pandas       as pd
import xarray       as xr
import sys
import os
from   datetime     import datetime


class candex:

    def __init__(self):
        self.case_name                 =  'case_temp' # name of the case
        self.target_shp                =  '' # sink/target shapefile
        self.target_shp_ID             =  '' # name of the column ID in the sink/target shapefile
        self.target_shp_lat            =  '' # name of the column latitude in the sink/target shapefile
        self.target_shp_lon            =  '' # name of the column longitude in the sink/target shapefile
        self.source_nc                 =  '' # name of nc file to be remapped
        self.var_names                 =  [] # list of varibale names to be remapped from the source NetCDF file
        self.var_lon                   =  '' # name of varibale longitude in the source NetCDF file
        self.var_lat                   =  '' # name of varibale latitude in the source NetCDF file
        self.var_time                  =  'time' # name of varibale time in the source NetCDF file
        self.var_ID                    =  '' # name of vriable ID in the source NetCDF file
        self.var_names_remapped        =  [] # list of varibale names that will be replaced in the remapped file
        self.source_shp                =  '' # name of source shapefile (essential for case-3)
        self.source_shp_lat            =  '' # name of column latitude in the source shapefile
        self.source_shp_lon            =  '' # name of column longitude in the source shapefile
        self.source_shp_ID             =  '' # name of column ID in the source shapefile
        self.remapped_var_id           =  'ID' # name of the ID variable in the new nc file; default 'ID'
        self.remapped_var_lat          =  'latitude' # name of the latitude variable in the new nc file; default 'latitude'
        self.remapped_var_lon          =  'longitude' # name of the longitude variable in the new nc file; default 'longitude'
        self.remapped_dim_id           =  'ID' # name of the ID dimension in the new nc file; default 'ID'
        self.temp_dir                  =  './temp/' # temp_dir
        self.output_dir                =  './output' # output directory
        self.format_list               =  ['f8'] # float for the remapped values
        self.fill_value_list           =  ['-9999'] # missing values set to -9999
        self.remap_csv                 =  '' # name of the remapped file if provided
        self.author_name               =  '' # name of the authour
        self.license                   =  '' # data license
        self.tolerance                 =  10**-5 # tolerance
        self.save_csv                  =  False # save csv

    def run_candex(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function runs a set of candex function which can remap from a srouce shapefile
        with regular, roated, irregular to a target shapefile
        """
        # check candex input
        self.check_candex_input()
        # if remap is not provided then create the remapping file
        if self.remap_csv == '':
            import geopandas as gpd
            # check the target shapefile
            target_shp_gpd = gpd.read_file(self.target_shp)
            target_shp_gpd = self.check_target_shp(target_shp_gpd)
            # save the standard target shapefile
            print('candex will save standard shapefile for candex claculation as:')
            print(self.temp_dir+self.case_name+'_target_shapefile.shp')
            target_shp_gpd.to_file(self.temp_dir+self.case_name+'_target_shapefile.shp') # save
            # check the source NetCDF files
            self.check_source_nc()
            # find the case
            self.NetCDF_SHP_lat_lon()
            # create the source shapefile for case 1 and 2 if shapefile is not provided
            if (self.case == 1 or self.case == 2)  and (self.source_shp == ''):
                if self.case == 1:
                    self.lat_lon_SHP(self.lat_expanded, self.lon_expanded,\
                        self.temp_dir+self.case_name+'_source_shapefile.shp')
                else:
                    self.lat_lon_SHP(self.lat, self.lon,\
                        self.temp_dir+self.case_name+'_source_shapefile.shp')
                print('candex is creating the shapefile from the netCDF file and saving it here:')
                print(self.temp_dir+self.case_name+'_source_shapefile.shp')
            if (self.case == 1 or self.case == 2)  and (self.source_shp != ''):
                source_shp_gpd = gpd.read_file(self.source_shp)
                source_shp_gpd = self.add_lat_lon_source_SHP(source_shp_gpd, self.source_shp_lat,\
                    self.source_shp_lon, self.source_shp_ID)
                source_shp_gpd.to_file(self.temp_dir+self.case_name+'_source_shapefile.shp')
                print('candex detect the shapefile is provided and will resave it here:')
                print(self.temp_dir+self.case_name+'_source_shapefile.shp')
            # if case 3 or source shapefile is provided
            if (self.case == 3) and (self.source_shp != ''):
                self.check_source_nc_shp() # check the lat lon in soure shapefile and nc file
                source_shp_gpd = gpd.read_file(self.source_shp)
                source_shp_gpd = self.add_lat_lon_source_SHP(source_shp_gpd, self.source_shp_lat,\
                    self.source_shp_lon, self.source_shp_ID)
                source_shp_gpd.to_file(self.temp_dir+self.case_name+'_source_shapefile.shp')
                print('candex is creating the shapefile from the netCDF file and saving it here:')
                print(self.temp_dir+self.case_name+'_source_shapefile.shp')
            # expand source shapefile
            source_shp_gpd = gpd.read_file(self.temp_dir+self.case_name+'_source_shapefile.shp')
            source_shp_gpd = source_shp_gpd.set_crs("EPSG:4326")
            expanded_source = self.expand_source_SHP(source_shp_gpd, self.temp_dir, self.case_name)
            expanded_source.to_file(self.temp_dir+self.case_name+'_source_shapefile_expanded.shp')
            # intersection of the source and sink/target shapefile
            shp_1 = gpd.read_file(self.temp_dir+self.case_name+'_target_shapefile.shp')
            shp_2 = gpd.read_file(self.temp_dir+self.case_name+'_source_shapefile_expanded.shp')
            # subset the extended shapefile based on the sink/target shapefile
            min_lon, min_lat, max_lon, max_lat = shp_1.total_bounds
            shp_2 ['lat_temp'] = shp_2.centroid.y
            shp_2 ['lon_temp'] = shp_2.centroid.x
            if (-180<min_lon) and max_lon<180:
                shp_2 = shp_2 [shp_2['lon_temp'] <=  180]
                shp_2 = shp_2 [-180 <= shp_2['lon_temp']]
            if (0<min_lon) and max_lon<360:
                shp_2 = shp_2 [shp_2['lon_temp'] <=  360]
                shp_2 = shp_2 [0    <= shp_2['lon_temp']]
            shp_2.drop(columns=['lat_temp', 'lon_temp'])
            # shp_2 = shp_2.reset_index(inplace=True, drop=True)
            # reprojections
            if (str(shp_1.crs).lower() == str(shp_2.crs).lower()) and ('epsg:4326' in str(shp_1.crs).lower()):
                shp_1 = shp_1.to_crs ("EPSG:6933") # project to equal area
                shp_1.to_file(self.temp_dir+self.case_name+'test.shp')
                shp_1 = gpd.read_file(self.temp_dir+self.case_name+'test.shp')
                shp_2 = shp_2.to_crs ("EPSG:6933") # project to equal area
                shp_2.to_file(self.temp_dir+self.case_name+'test.shp')
                shp_2 = gpd.read_file(self.temp_dir+self.case_name+'test.shp')
                # remove test files
                removeThese = glob.glob(self.temp_dir+self.case_name+'test.*')
                for file in removeThese:
                    os.remove(file)
            shp_int = self.intersection_shp(shp_1, shp_2)
            shp_int = shp_int.sort_values(by=['S_1_ID_t']) # sort based on ID_t
            shp_int = shp_int.to_crs ("EPSG:4326") # project back to WGS84
            shp_int.to_file(self.temp_dir+self.case_name+'_intersected_shapefile.shp') # save the intersected files
            shp_int = shp_int.drop(columns=['geometry']) # remove the geometry
            # rename dictionary
            dict_rename = {'S_1_ID_t' : 'ID_t',
                           'S_1_lat_t': 'lat_t',
                           'S_1_lon_t': 'lon_t',
                           'S_2_ID_s' : 'ID_s',
                           'S_2_lat_s': 'lat_s',
                           'S_2_lon_s': 'lon_s',
                           'AP1N'     : 'weight'}
            shp_int = shp_int.rename(columns=dict_rename) # rename fields for remapping file
            shp_int = pd.DataFrame(shp_int) # move to data set and save as a csv
            shp_int.to_csv(self.temp_dir+self.case_name+'_intersected_shapefile.csv') # save the intersected files
            # create the remap file if remap file
            int_df = pd.read_csv (self.temp_dir+self.case_name+'_intersected_shapefile.csv')
            lat_source = self.lat
            lon_source = self.lon
            int_df = self.create_remap(int_df, lat_source, lon_source)
            int_df.to_csv(self.temp_dir+self.case_name+'_remapping.csv')
            self.remap_csv = self.temp_dir+self.case_name+'_remapping.csv'
        else:
            # check the remap file if provided
            int_df  = pd.read_csv(self.remap_csv)
            self.check_candex_remap(int_df)
            # check the source nc file
            self.check_source_nc()
        self.__target_nc_creation()

    def get_col_row(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates the dataframe with assosiated latitude and longitude or source file and
        its location of data by column and row for candex to extract/remap data
        """
        # find the case
        self.NetCDF_SHP_lat_lon()
        #
        lat_target_int = np.array(self.lat); lat_target_int = lat_target_int.flatten()
        lon_target_int = np.array(self.lon); lon_target_int = lon_target_int.flatten()
        rows, cols = self.create_row_col_df (self.lat, self.lon, lat_target_int, lon_target_int)
        # create the data frame
        lat_lon_row_col = pd.DataFrame()
        lat_lon_row_col ['lat_s'] = lat_target_int
        lat_lon_row_col ['lon_s'] = lon_target_int
        lat_lon_row_col ['rows']  = rows
        lat_lon_row_col ['cols']  = cols
        # saving
        lat_lon_row_col.to_csv(self.temp_dir+self.case_name+'_row_col_lat_lon.csv')
        self.col_row_name = self.temp_dir+self.case_name+'_row_col_lat_lon.csv'

    def check_candex_input(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        the functions checkes if the necessary candex object are provided from the user
        """
        if self.temp_dir != '':
            if self.temp_dir[-1] != '/':
                sys.exit('the provided temporary folder for candex should end with (/)')
            if not os.path.isdir(self.temp_dir):
                os.mkdir(self.temp_dir)
        if self.output_dir == '':
            sys.exit('the provided folder for candex remapped netCDF output is missing; please provide that')
        if self.output_dir != '':
            if self.output_dir[-1] != '/':
                sys.exit('the provided output folder for candex should end with (/)')
            if not os.path.isdir(self.output_dir):
                os.mkdir(self.output_dir)
        if self.temp_dir == '':
            print("No temporary folder is provided for candex; this will result in candex saving the files in the same directory as python script")
        if self.author_name == '':
            print("no author name is provide and the author name is changed to (author name)!")
            self.author_name = "author name"
        if (len(self.var_names) != 1) and (len(self.format_list) == 1) and (len(self.fill_value_list) ==1):
            if (len(self.var_names) != len(self.fill_value_list)) and \
            (len(self.var_names) != len(self.format_list)) and \
            (len(self.format_list) == 1) and (len(self.fill_value_list) ==1):
                print('candex is given multiple varibales to be remapped but only on format and fill value'+\
                    'candex repeat the format and fill value for all the variables in output files')
                self.format_list     = self.format_list     * len(self.var_names)
                self.fill_value_list = self.fill_value_list * len(self.var_names)
            else:
                sys.exit('number of varibales and fill values and formats do not match')
        if self.remap_csv != '':
            print('remap file is provided; candex will use this file and skip calculation of remapping')
        if len(self.var_names) != len(set(self.var_names)):
            sys.exit('the name of the variables you have provided from the source NetCDF file to be remapped are not unique')
        if self.var_names_remapped:
            if len(self.var_names_remapped) != len(set(self.var_names_remapped)):
                sys.exit('the name of the variables you have provided as the rename in the remapped file are not unique')
            if len(self.var_names_remapped) != len(self.var_names):
                sys.exit('the number of provided variables from the source file and names to be remapped are not the same length')
        if not self.var_names_remapped:
            self.var_names_remapped = self.var_names
        for i in np.arange(len(self.var_names)):
            print('candex will remap variable ',self.var_names[i],' from source file to variable ',self.var_names_remapped[i],' in remapped NeCDF file')

    def check_target_shp (self,shp):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        this function check if target shapefile and add ID and centroid lat and lon is not provided
        Arguments
        ---------
        shp: geopandas dataframe, polygone, multipolygon, point, multipoint
        """
        # load the needed packages
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        # sink/target shapefile check the projection
        if 'epsg:4326' not in str(shp.crs).lower():
            sys.exit('please project your shapefile to WGS84 (epsg:4326)')
        else: # check if the projection is WGS84 (or epsg:4326)
            print('candex detects that target shapefile is in WGS84 (epsg:4326)')
        # check if the ID, latitude, longitude are provided
        if self.target_shp_ID == '':
            print('candex detects that no field for ID is provided in sink/target shapefile')
            print('arbitarary values of ID are added in the field ID_t')
            shp['ID_t']  = np.arange(len(shp))+1
        else:
            print('candex detects that the field for ID is provided in sink/target shapefile')
            # check if the provided IDs are unique
            ID_values = np.array(shp[self.target_shp_ID])
            if len(ID_values) != len(np.unique(ID_values)):
                sys.exit('The provided IDs in shapefile are not unique; provide unique IDs or do not identify target_shp_ID')
            shp['ID_t'] = shp[self.target_shp_ID]
        if self.target_shp_lat == '' or self.target_shp_lon == '':
            print('candex detects that either of the fields for latitude or longitude is not provided in sink/target shapefile')
            print('calculating centroid of shapes in equal area projection')
            shp_temp = shp.to_crs ("EPSG:6933") # source shapefile to equal area
            lat_c = np.array(shp_temp.centroid.y) # centroid lat from target
            lon_c = np.array(shp_temp.centroid.x) # centroid lon from target
            ID = np.array(shp['ID_t'])
            df_point = pd.DataFrame()
            df_point ['lat'] = lat_c
            df_point ['lon'] = lon_c
            df_point ['ID']  = ID
            shp_points = self.make_shape_point(df_point, 'lon', 'lat')
            shp_points = shp_points.set_crs("EPSG:6933") # set equal area
            shp_points = shp_points.to_crs("EPSG:4326") # to WGS
            shp_points = shp_points.drop(columns=['lat','lon'])
            shp_points ['lat'] = shp_points.geometry.y
            shp_points ['lon'] = shp_points.geometry.x
            shp_points.to_file(self.temp_dir+self.case_name+'_centroid.shp') # save
            print('point shapefile for centroid of the shapes is saves here:')
            print(self.temp_dir+self.case_name+'_centroid.shp')
        if self.target_shp_lat == '':
            print('candex detects that no field for latitude is provided in sink/target shapefile')
            print('latitude values are added in the field lat_t')
            shp['lat_t']  = shp_points ['lat'] # centroid lat from target
        else:
            print('candex detects that the field latitude is provided in sink/target shapefile')
            shp['lat_t'] = shp[self.target_shp_lat]
        if self.target_shp_lon == '':
            print('candex detects that no field for longitude is provided in sink/target shapefile')
            print('longitude values are added in the field lon_t')
            shp['lon_t']  = shp_points ['lon'] # centroid lon from target
        else:
            print('candex detects that the field longitude is provided in sink/target shapefile')
            shp['lon_t'] = shp[self.target_shp_lat]
        # check other geometries and add buffer if needed
        detected_points = False
        detected_multipoints = False
        detected_lines = False
        for index, _ in shp.iterrows():
            polys = shp.geometry.iloc[index] # get the shape
            if polys.geom_type.lower() == "point"      or polys.geom_type.lower() == "points":
                detected_points = True
                polys = polys.buffer(10**-5).simplify(10**-5)
                shp.geometry.iloc[index] = polys
            if polys.geom_type.lower() == "multipoint" or polys.geom_type.lower() == "multipoints":
                detected_multipoints = True
                polys = polys.buffer(10**-5).simplify(10**-5)
                shp.geometry.iloc[index] = polys
            if polys.geom_type.lower() == "line"       or polys.geom_type.lower() == "lines":
                detected_lines = True
                polys = polys.buffer(10**-5).simplify(10**-5)
                shp.geometry.iloc[index] = polys
            if polys.geom_type.lower() == "polyline"   or polys.geom_type.lower() == "polylines":
                detected_lines = True
                polys = polys.buffer(10**-5).simplify(10**-5)
                shp.geometry.iloc[index] = polys
        # print messages
        if detected_points:
            print('candex detects point(s) as geometry of target shapefile and will apply small buffer to them')
        if detected_multipoints:
            print('candex detected multipoint as geometry of target shapefile and will considere it as multipolygone')
            print('hence candex will provide the average of all the point in each multipoint')
            print('if you mistakenly have given poitns as multipoints please correct the target shapefile')
        if detected_lines:
            print('candex detected line as geometry of target shapefile and will considere it as polygon (adding small buffer)')
        print('it seems everything is OK with the sink/target shapefile; added to candex object target_shp_gpd')
        return shp

    def check_source_nc (self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks the consistency of the dimentions and varibales for source netcdf file(s)
        """
        flag_do_not_match = False
        nc_names = glob.glob (self.source_nc)
        if not nc_names:
            sys.exit('candex detects no netCDF file; check the path to the soure netCDF files')
        else:
            ncid      = nc4.Dataset(nc_names[0])
            var_dim   = list(ncid.variables[self.var_names[0]].dimensions)
            lat_dim   = list(ncid.variables[self.var_lat].dimensions)
            lon_dim   = list(ncid.variables[self.var_lon].dimensions)
            lat_value = np.array(ncid.variables[self.var_lat])
            lon_value = np.array(ncid.variables[self.var_lon])
            # dimension check based on the first netcdf file
            if not (set(lat_dim) <= set(var_dim)):
                flag_do_not_match = True
            if not (set(lon_dim) <= set(var_dim)):
                flag_do_not_match = True
            if (len(lat_dim) == 2) and (len(lon_dim) == 2) and (len(var_dim) == 3): # case 2
                if not (set(lat_dim) == set(lon_dim)):
                    flag_do_not_match = True
            if (len(lat_dim) == 1) and (len(lon_dim) == 1) and (len(var_dim) == 2): # case 3
                if not (set(lat_dim) == set(lon_dim)):
                    flag_do_not_match = True
            # dimension check and consistancy for variable latitude
            for nc_name in nc_names:
                ncid = nc4.Dataset(nc_name)
                temp = list(ncid.variables[self.var_lat].dimensions)
                # fist check the length of the temp and lat_dim
                if len(temp) != len(lat_dim):
                    flag_do_not_match = True
                else:
                    for i in np.arange(len(temp)):
                        if temp[i] != lat_dim[i]:
                            flag_do_not_match = True
                temp = np.array(ncid.variables[self.var_lat])
                if np.sum(abs(lat_value-temp))>self.tolerance:
                    flag_do_not_match = True
            # dimension check and consistancy for variable longitude
            for nc_name in nc_names:
                ncid = nc4.Dataset(nc_name)
                temp = list(ncid.variables[self.var_lon].dimensions)
                # fist check the length of the temp and lon_dim
                if len(temp) != len(lon_dim):
                    flag_do_not_match = True
                else:
                    for i in np.arange(len(temp)):
                        if temp[i] != lon_dim[i]:
                            flag_do_not_match = True
                temp = np.array(ncid.variables[self.var_lon])
                if np.sum(abs(lon_value-temp))>self.tolerance:
                    flag_do_not_match = True
            # dimension check consistancy for variables to be remapped
            for var_name in self.var_names:
                # get the varibale information of lat, lon and dimensions of the varibale.
                for nc_name in nc_names:
                    ncid = nc4.Dataset(nc_name)
                    temp = list(ncid.variables[var_name].dimensions)
                    # fist check the length of the temp and var_dim
                    if len(temp) != len(var_dim):
                        flag_do_not_match = True
                    else:
                        for i in np.arange(len(temp)):
                            if temp[i] != var_dim[i]:
                                flag_do_not_match = True
            # check varibale time and dimension time are the same name so time is coordinate
            for nc_name in nc_names:
                ncid = nc4.Dataset(nc_name)
                temp = ncid.variables[self.var_time].dimensions
                if len(temp) != 1:
                    sys.exit('candex expects 1D time varibale, it seems time varibales has more than 1 dimension')
                if str(temp[0]) != self.var_time:
                    sys.exit('candex expects time varibale and dimension to be different, they should be the same\
                    for xarray to consider time dimension as coordinates')
        if flag_do_not_match:
            sys.exit('candex detects that all the provided netCDF files and varibale \
has different dimensions for the varibales or latitude and longitude')
        else:
            print('candex detects that the varibales from the netCDF files are identical\
in dimensions of the varibales and latitude and longitude')
            print('candex detects that all the varibales have dimensions of:')
            print(var_dim)
            print('candex detects that the longitude varibales has dimensions of:')
            print(lon_dim)
            print('candex detects that the latitude varibales has dimensions of:')
            print(lat_dim)

    def check_source_nc_shp (self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks the source netcdf file shapefile
        needs more development
        """
        # load the needed packages
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        #
        multi_source = False
        nc_names = glob.glob (self.source_nc)
        ncid = nc4.Dataset(nc_names[0])
        # sink/target shapefile is what we want the varibales to be remapped to
        shp = gpd.read_file(self.source_shp)
        if 'epsg:4326' not in str(shp.crs).lower():
            sys.exit('please project your source shapefile and varibales in source nc files to WGS84 (epsg:4326)')
        else: # check if the projection is WGS84 (or epsg:4326)
            print('candex detects that source shapefile is in WGS84 (epsg:4326)')
        # get the lat/lon from source shapfile and nc files
        lat_shp = np.array(shp[self.source_shp_lat]); lat_shp = lat_shp.astype(float)
        lon_shp = np.array(shp[self.source_shp_lon]); lon_shp = lon_shp.astype(float)
        #
        coord_shp         = pd.DataFrame()
        coord_shp ['lon'] = lon_shp
        coord_shp ['lat'] = lat_shp
        coord_shp         = coord_shp.sort_values(by='lon')
        coord_shp         = coord_shp.sort_values(by='lat')
        # check if all the lat/lon in shapefile are unique
        df_temp           = coord_shp.drop_duplicates()
        if len(df_temp)  != len(coord_shp):
            # print
            sys.exit('The latitude and longitude in source shapefile are not unique')
        # first check the model
        coord_nc          = pd.DataFrame()
        coord_nc  ['lon'] = self.lon
        coord_nc  ['lat'] = self.lat
        coord_nc          = coord_nc.sort_values(by='lon')
        coord_nc          = coord_nc.sort_values(by='lat')
        # check if all the lat/lon in shapefile are unique
        df_temp           = coord_nc.drop_duplicates()
        if len(df_temp)  != len(coord_nc):
            # print
            #sys.exit('The latitude and longitude in source NetCDF files are not unique')
            print('The latitude and longitude in source NetCDF files are not unique')
        # check if the latitude and longitude in the shapefiles are in the nc file
        # coord_nc_temp     = np.array(coord_nc)
        # for index, row in coord_shp.iterrows():
        #     # get the distance
        #     lat_target = row.lat
        #     lon_target = row.lon
        #     Dist = np.array(((coord_nc_temp[:,1] - lat_target)**2 +\
        #         (coord_nc_temp[:,0] - lon_target)**2)**0.5)
        #     idx_target = np.where(Dist<self.tolerance)
        #     idx_target = np.array(idx_target)
        #     idx_target = idx_target.flatten()
        #     if idx_target.shape[0] > 1:
        #         multi_source = True
        #         #idx_target    = max (idx_target[0]-1000,0)
        #         #sys.exit('There are multiple shape in source shapefile that is closer to provided netCDF latitude and longitude')
        #     if idx_target.shape[0] == 1:
        #         idx_target = idx_target.item()
        #         idx_target    = max (idx_target-1000,0)
        #     print(idx_target)
        #     coord_nc_temp = coord_nc_temp[idx_target:] # cut the
        # if multi_source:
        #     print('There are multiple shape in source shapefile that is closer to provided netCDF latitude and longitude')

    def NetCDF_SHP_lat_lon(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks dimension of the source shapefile and checks the case of regular, rotated, and irregular
        also created the 2D array of lat and lon for creating the shapefile
        """
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        #
        nc_names = glob.glob (self.source_nc)
        var_name = self.var_names[0]
        # open the nc file to read
        ncid = nc4.Dataset(nc_names[0])
        # deciding which case
        # case #1 regular latitude/longitude
        if (len(ncid.variables[self.var_lon].dimensions)==1) and\
        (len(ncid.variables[self.var_lon].dimensions)==1) and\
        (len(ncid.variables[self.var_names[0]].dimensions)==3):
            print('candex detects case 1 - regular lat/lon')
            self.case = 1
            # get the list of dimensions for the ncid sample varibale
            list_dim_name = list(ncid.variables[self.var_names[0]].dimensions)
            # get the location of lat dimensions
            location_of_lat = list_dim_name.index(list(ncid.variables[self.var_lat].dimensions)[0])
            locaiton_of_lon = list_dim_name.index(list(ncid.variables[self.var_lon].dimensions)[0])
            # det the dimensions of lat and lon
            len_of_lat = len(ncid.variables[self.var_lat][:])
            len_of_lon = len(ncid.variables[self.var_lon][:])
            if locaiton_of_lon > location_of_lat:
                lat = np.zeros([len_of_lat, len_of_lon])
                lon = np.zeros([len_of_lat, len_of_lon])
                for i in np.arange(len(ncid.variables[self.var_lon][:])):
                    lat [:,i] = ncid.variables[self.var_lat][:]
                for i in np.arange(len(ncid.variables[self.var_lat][:])):
                    lon [i,:] = ncid.variables[self.var_lon][:]
            else:
                lat = np.zeros([len_of_lon, len_of_lat])
                lon = np.zeros([len_of_lon, len_of_lat])
                for i in np.arange(len(ncid.variables[self.var_lon][:])):
                    lat [i,:] = ncid.variables[self.var_lat][:]
                for i in np.arange(len(ncid.variables[self.var_lat][:])):
                    lon [:,i] = ncid.variables[self.var_lon][:]
            # check if lat and lon are spaced equally
            lat_temp = np.array(ncid.variables[self.var_lat][:])
            lat_temp_diff = np.diff(lat_temp)
            lat_temp_diff_unique = np.unique(lat_temp_diff)
            #print(lat_temp_diff_unique)
            #print(lat_temp_diff_unique.shape)
            #
            lon_temp = np.array(ncid.variables[self.var_lon][:])
            lon_temp_diff = np.diff(lon_temp)
            lon_temp_diff_unique = np.unique(lon_temp_diff)
            #print(lon_temp_diff_unique)
            #print(lon_temp_diff_unique.shape)
            # expanding just for the the creation of shapefile with first last rows and columns
            if (len(lat_temp_diff_unique)==1) and (len(lon_temp_diff_unique)==1): # then lat lon are spaced equal
                # create expanded lat
                lat_expanded = np.zeros(np.array(lat.shape)+2)
                lat_expanded [1:-1,1:-1] = lat
                lat_expanded [:, 0]  = lat_expanded [:, 1] + (lat_expanded [:, 1] - lat_expanded [:, 2]) # populate left column
                lat_expanded [:,-1]  = lat_expanded [:,-2] + (lat_expanded [:,-2] - lat_expanded [:,-3]) # populate right column
                lat_expanded [0, :]  = lat_expanded [1, :] + (lat_expanded [1, :] - lat_expanded [2, :]) # populate top row
                lat_expanded [-1,:]  = lat_expanded [-2,:] + (lat_expanded [-2,:] - lat_expanded [-3,:]) # populate bottom row
                # create expanded lat
                lon_expanded = np.zeros(np.array(lon.shape)+2)
                lon_expanded [1:-1,1:-1] = lon
                lon_expanded [:, 0]  = lon_expanded [:, 1] + (lon_expanded [:, 1] - lon_expanded [:, 2]) # populate left column
                lon_expanded [:,-1]  = lon_expanded [:,-2] + (lon_expanded [:,-2] - lon_expanded [:,-3]) # populate right column
                lon_expanded [0, :]  = lon_expanded [1, :] + (lon_expanded [1, :] - lon_expanded [2, :]) # populate top row
                lon_expanded [-1,:]  = lon_expanded [-2,:] + (lon_expanded [-2,:] - lon_expanded [-3,:]) # populate bottom row
            #
            lat      = np.array(lat).astype(float)
            lon      = np.array(lon).astype(float)
            self.lat = lat
            self.lon = lon
            self.lat_expanded = lat_expanded
            self.lon_expanded = lon_expanded
        # case #2 rotated lat/lon
        if (len(ncid.variables[self.var_lat].dimensions)==2) and (len(ncid.variables[self.var_lon].dimensions)==2):
            print('candex detects case 2 - rotated lat/lon')
            self.case = 2
            lat = ncid.variables[self.var_lat][:,:]
            lon = ncid.variables[self.var_lon][:,:]
            # creating/saving the shapefile
            lat = np.array(lat).astype(float)
            lon = np.array(lon).astype(float)
            self.lat = lat
            self.lon = lon
        # case #3 1-D lat/lon and 2 data for irregulat shapes
        if (len(ncid.variables[self.var_lat].dimensions)==1) and (len(ncid.variables[self.var_lon].dimensions)==1) and\
           (len(ncid.variables[self.var_names[0]].dimensions)==2):
            print('candex detects case 3 - irregular lat/lon; shapefile should be provided')
            self.case = 3
            lat = ncid.variables[self.var_lat][:]
            lon = ncid.variables[self.var_lon][:]
            #print(lat, lon)
            if self.var_ID  == '':
                print('candex detects that no varibale for ID of the source netCDF file; an arbitatiry ID will be provided')
                ID =  np.arange(len(lat))+1 # pass arbitarary values
            else:
                ID = ncid.variables[self.var_ID][:]
            # creating/saving the shapefile
            lat = np.array(lat).astype(float)
            lon = np.array(lon).astype(float)
            self.lat = lat
            self.lon = lon
            self.ID  = ID

    def lat_lon_SHP(self,
                    lat,
                    lon,
                    file_name):
        """
        @ author:                  Shervan Gharari, Wouter Knoben
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a shapefile for the source netcdf file
        Arguments
        ---------
        lat: the 2D matrix of lat_2D [n,m,]
        lon: the 2D matrix of lon_2D [n,m,]
        file_name: string, name of the file that the shapefile will be saved at
        """
        # check if lat/lon that are taken in has the same dimension
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        #
        lat_lon_shape = lat.shape
        # write the shapefile
        with shapefile.Writer(file_name) as w:
            w.autoBalance = 1 # turn on function that keeps file stable if number of shapes and records don't line up
            w.field("ID_s",'N') # create (N)umerical attribute fields, integer
            w.field("lat_s",'F',decimal=4) # float with 4 decimals
            w.field("lon_s",'F',decimal=4) # float with 4 decimals
            # preparing the m whcih is a couter for the shapefile arbitrary ID
            m = 0.00
            # itterating to create the shapes of the result shapefile ignoring the first and last rows and columns
            for i in range(1, lat_lon_shape[0] - 1):
                for j in range(1, lat_lon_shape[1] - 1):
                    # checking is lat and lon is located inside the provided bo
                    # empty the polygon variable
                    parts = []
                    # update records
                    m += 1 # ID
                    center_lat = lat[i,j] # lat value of data point in source .nc file
                    center_lon = lon[i,j] # lon value of data point in source .nc file should be within [0,360]
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

    def add_lat_lon_source_SHP( self,
                                shp,
                                source_shp_lat,
                                source_shp_lon,
                                source_shp_ID):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function add lat, lon and ID from the source shapefile if provided
        Arguments
        ---------
        shp: geodataframe of source shapefile
        source_shp_lat: string, the name of the lat field in the source shapefile
        source_shp_lon: string, the name of the lon field in the source shapefile
        source_shp_ID: string, the name of the ID field in the source shapefile
        """
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        shp['lat_s'] = shp [source_shp_lat].astype(float)
        shp['lon_s'] = shp [source_shp_lon].astype(float)
        if self.source_shp_ID != '':
            shp ['ID_s']  = shp [source_shp_ID]
        else:
            shp ['ID_s']  = np.arange(len(shp))+1
        return shp

    def expand_source_SHP(  self,
                            shp,
                            temp_dir,
                            case_name):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function add lat, lon and ID from the source shapefile if provided
        Arguments
        ---------
        shp: geodataframe of source shapefile
        source_shp_lat: string, the name of the lat field in the source shapefile
        source_shp_lon: string, the name of the lon field in the source shapefile
        source_shp_ID: string, the name of the ID field in the source shapefile
        Returns
        -------
        result: geopandas dataframe, expanded source shapefile that covers -180 to 360 of lon
        """
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        # put a check to see if there is lon_s and lat_s, ID_s
        column_names = shp.columns
        column_names = list(column_names)
        if ('lon_s' not in column_names) or ('lat_s' not in column_names) or ('ID_s' not in column_names):
            sys.exit('the source shapefile does not have column of lon_s lat_s and ID_s')
        # check if the shapefile is already expanded
        min_lon, min_lat, max_lon, max_lat = shp.total_bounds
        print(min_lon, min_lat, max_lon, max_lat)
        if (180 < max_lon) and (min_lon < 0):
            print('it seems the source shapefile is already expanded between -180 to 360 longitude')
        else:
            if max (shp['lon_s']) > 180 and min (shp['lon_s']) > 0 and max (shp['lon_s']) < 360:
                print('candex decides the netCDF file has longtitude values of 0 to 360; creating the extended')
                shp1 = shp [shp['lon_s'] <= 180]
                shp2 = shp [shp['lon_s'] >  180]
                if not shp1.empty:
                    shp1.to_file(temp_dir+case_name+'_source_shapefileA.shp')
                    shp1 = gpd.read_file(temp_dir+case_name+'_source_shapefileA.shp')
                    temp = shp1
                if not shp2.empty:
                    shp2.to_file(temp_dir+case_name+'_source_shapefileB.shp')
                    shp2 = gpd.read_file(temp_dir+case_name+'_source_shapefileB.shp')
                    shp3 = gpd.read_file(temp_dir+case_name+'_source_shapefileB.shp')
                    # loop change the geometry
                    for index, _ in shp3.iterrows():
                        polys = shp3.geometry.iloc[index] # get the shape
                        polys = shapely.affinity.translate(polys, xoff=-360.0, yoff=0.0, zoff=0.0)
                        shp3.geometry.iloc[index] = polys
                    shp3.to_file(temp_dir+case_name+'_source_shapefileC.shp')
                    shp3 = gpd.read_file(temp_dir+case_name+'_source_shapefileC.shp')
                    temp = gpd.GeoDataFrame( pd.concat( [shp3,shp2] , ignore_index=True  ) )
                    if not shp1.empty:
                        temp = gpd.GeoDataFrame( pd.concat( [temp,shp1] , ignore_index=True )  )
                temp.to_file(temp_dir+case_name+'_source_shapefile_expanded.shp')
            # the netcdf file has values between -180 to 180
            elif min (shp['lon_s']) > -180 and max (shp['lon_s']) < 180:
                print('candex decides the netCDF file has longtitude values of -180 to 180; creating the extended')
                shp1 = shp [shp['lon_s'] >   0]
                shp2 = shp [shp['lon_s'] <=  0]
                if not shp1.empty:
                    shp1.to_file(temp_dir+case_name+'_source_shapefileA.shp')
                    shp1 = gpd.read_file(temp_dir+case_name+'_source_shapefileA.shp')
                    temp = shp1
                if not shp2.empty:
                    shp2.to_file(temp_dir+case_name+'_source_shapefileB.shp')
                    shp2 = gpd.read_file(temp_dir+case_name+'_source_shapefileB.shp')
                    shp3 = gpd.read_file(temp_dir+case_name+'_source_shapefileB.shp')
                    # loop change the geometry
                    for index, _ in shp3.iterrows():
                        polys = shp3.geometry.iloc[index] # get the shape
                        polys = shapely.affinity.translate(polys, xoff=+360.0, yoff=0.0, zoff=0.0)
                        shp3.geometry.iloc[index] = polys
                    shp3.to_file(temp_dir+case_name+'_source_shapefileC.shp')
                    shp3 = gpd.read_file(temp_dir+case_name+'_source_shapefileC.shp')
                    temp = gpd.GeoDataFrame( pd.concat( [shp3,shp2] , ignore_index=True  ) )
                    if not shp1.empty:
                        temp = gpd.GeoDataFrame( pd.concat( [temp,shp1] , ignore_index=True )  )
                temp.to_file(temp_dir+case_name+'_source_shapefile_expanded.shp')
            else:
                sys.exit('candex cannot decide about the lat and lon of the shapefiles')
        result = gpd.read_file(temp_dir+case_name+'_source_shapefile_expanded.shp')
        return result

    def intersection_shp(   self,
                            shp_1,
                            shp_2):
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @license:                  GNU-GPLv3
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
        shp_1: geo data frame, shapefile 1
        shp_2: geo data frame, shapefile 2
        Returns
        -------
        result: a geo data frame that includes the intersected shapefile and area, percent and normalized percent of each shape
        elements in another one
        """
        # get the column name of shp_1
        column_names = shp_1.columns
        column_names = list(column_names)
        # removing the geometry from the column names
        column_names.remove('geometry')
        # renaming the column with S_1
        for i in range(len(column_names)):
            shp_1 = shp_1.rename(
                columns={column_names[i]: 'S_1_' + column_names[i]})
        # Caclulating the area for shp1
        shp_1['AS1']  = shp_1.area
        shp_1['IDS1'] = np.arange(shp_1.shape[0])+1
        # get the column name of shp_2
        column_names = shp_2.columns
        column_names = list(column_names)
        # removing the geometry from the colomn names
        column_names.remove('geometry')
        # renaming the column with S_2
        for i in range(len(column_names)):
            shp_2 = shp_2.rename(
                columns={column_names[i]: 'S_2_' + column_names[i]})
        # Caclulating the area for shp2
        shp_2['AS2'] = shp_2.area
        shp_2['IDS2'] = np.arange(shp_2.shape[0])+1
        # making intesection
        result = self.spatial_overlays (shp_1, shp_2, how='intersection')
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

    def spatial_overlays(   self,
                            df1,
                            df2,
                            how='intersection',
                            reproject=True):
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        """
        Perform spatial overlay between two polygons.
        Currently only supports data GeoDataFrames with polygons.
        Implements several methods that are all effectively subsets of
        the union.
        author: Omer Ozak
        https://github.com/ozak
        https://github.com/geopandas/geopandas/pull/338
        license: GNU-GPLv3
        Parameters
        ----------
        df1: GeoDataFrame with MultiPolygon or Polygon geometry column
        df2: GeoDataFrame with MultiPolygon or Polygon geometry column
        how: string
            Method of spatial overlay: 'intersection', 'union',
            'identity', 'symmetric_difference' or 'difference'.
        use_sindex : boolean, default True
            Use the spatial index to speed up operation if available.
        Returns
        -------
        df: GeoDataFrame
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
            df1['sidx'] = df1.bbox.apply(lambda x:list(spatial_index.intersection(x)))
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

    def create_remap(   self,
                        int_df,
                        lat_source,
                        lon_source):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        this function add the corresponsing row and columns from the source NetCDF file
        Parameters
        ----------
        int_df: intersected data frame that includes the infromation for source and sink
        lat_source: numpy array of source lat
        lon_source: numpy array of source lon
        Returns
        -------
        int_df: dataframe, including the associated rows and cols and candex case
        """
        # the lat lon from the intersection/remap
        lat_source_int = np.array(int_df['lat_s'])
        lon_source_int = np.array(int_df['lon_s'])
        # call get row and col function
        rows, cols = self.create_row_col_df (lat_source, lon_source, lat_source_int, lon_source_int)
        # add rows and columns
        int_df['rows'] = rows
        int_df['cols'] = cols
        # pass the case to the remap_df
        int_df['candex_case'] = self.case
        # save remap_df as csv for future use
        return int_df

    def create_row_col_df ( self,
                            lat_source,
                            lon_source,
                            lat_target_int,
                            lon_target_int):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        this fucntion gets the row and colomns of the source netcdf file and returns it
        ----------
        lat_source: numpy array of lat source
        lon_source: numpy array of lon source
        lat_target_int: numpy array of lat source
        lon_target_int: numpy array of lon source
        Returns
        -------
        rows: numpy array, rows from the source file based on the target lat/lon
        cols: numpy array, cols from the source file based on the target lat/lon
        """
        # create the rows and cols
        rows = np.zeros(len(lat_target_int))
        cols = np.zeros(len(lon_target_int))
        # loop to find the rows and colomns
        for i in np.arange(len(lat_target_int)):
            lat_lon_value_diff = abs(lat_target_int[i]-lat_source)+abs(lon_target_int[i]-lon_source)
            if self.case == 1 or self.case == 2:
                row, col = np.where(lat_lon_value_diff == np.min(lat_lon_value_diff))
            if self.case == 3:
                row = np.where(lat_lon_value_diff == np.min(lat_lon_value_diff))
                col = row
            rows [i] = row[0]
            cols [i] = col[0]
        # pass to class
        return rows, cols

    def check_candex_remap(  self,
                             remap_df):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        this function check the remapping dataframe
        Parameters:
        ----------
        remap_df: dataframe, including remapping information including the following colomns:
                    ID_target
                    lon_target
                    lat_target
                    ID_source
                    lat_source
                    lon_source
                    rows
                    cols
                    order
        """
        # check if there is candex_case in the columns
        if 'candex_case' in remap_df.columns:
            print('candex case exists in the remap file')
        else:
            sys.exit('candex case field do not esits in the remap file; make sure to include this and take care if your do it manually!')
        # check if all the candex_case is unique for the data set
        if not (len(np.unique(np.array(remap_df['candex_case'])))==1):
            sys.exit('the candex_case is not unique in the remapping file')
        if not (np.unique(np.array(remap_df['candex_case'])) == 1 or\
        np.unique(np.array(remap_df['candex_case'])) == 2 or\
        np.unique(np.array(remap_df['candex_case'])) == 3):
            sys.exit('candex case should be one of 1, 2 or 3; please refer to the documentation')
        self.case = np.unique(np.array(remap_df['candex_case']))
        # check if the needed columns are existing
        if not set(['ID_t','lat_t','lon_t','ID_s','lat_s','lon_s','weight']) <= set(remap_df.columns):
            sys.exit('provided remapping file does not have one of the needed fields: \n'+\
                'ID_t, lat_t, lon_t, ID_2, lat_s, lon_s, weight')

    def __target_nc_creation(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This funciton read different grids and sum them up based on the
        weight provided to aggregate them over a larger area
        """
        print('------REMAPPING------')
        remap = pd.read_csv(self.remap_csv)
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
        self.rows = np.array(remap['rows']).astype(int)
        self.cols = np.array(remap['cols']).astype(int)
        self.number_of_target_elements = len(hruID_var)
        #
        nc_names = glob.glob(self.source_nc)
        nc_names = sorted(nc_names)
        for nc_name in nc_names:
            # get the time unit and time var from source
            ncids = nc4.Dataset(nc_name)
            # Check data license, calendar and time units
            nc_att_list = ncids.ncattrs()
            nc_att_list = [each_att for each_att in nc_att_list]
            nc_att_list_lower = [each_att.lower() for each_att in nc_att_list]
            if self.license == '' and ('license' not in nc_att_list_lower):
                self.license == 'the original license of the source NetCDF file is not provided'
            if ('license' in nc_att_list_lower):
                if 'license' in nc_att_list:
                    self.license == getattr(ncids, 'license')
                if 'License' in nc_att_list:
                    self.license == getattr(ncids, 'License')
                if 'LICENSE' in nc_att_list:
                    self.license == getattr(ncids, 'LICENSE')
                self.license == 'Original data license '+self.license
            if 'units' in ncids.variables[self.var_time].ncattrs():
                time_unit = ncids.variables[self.var_time].units
            else:
                sys.exit('units is not provided for the time varibale for source NetCDF of'+ nc_name)
            if 'calendar' in ncids.variables[self.var_time].ncattrs():
                time_cal = ncids.variables[self.var_time].calendar
            else:
                sys.exit('calendar is not provided for the time varibale for source NetCDF of'+ nc_name)
            time_var = ncids[self.var_time][:]
            self.length_of_time = len(time_var)
            target_date_times = nc4.num2date(time_var,units = time_unit,calendar = time_cal)
            target_name = self.output_dir + self.case_name + '_remapped_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.nc'
            if os.path.exists(target_name): # remove file if exists
                os.remove(target_name)
            for var in ncids.variables.values():
                if var.name == self.var_time:
                    time_dtype =  str(var.dtype)
            time_dtype_code = 'f8' # initialize the time as float
            if 'float' in time_dtype.lower():
                time_dtype_code = 'f8'
            elif 'int' in time_dtype.lower():
                time_dtype_code = 'i4'
            # reporting
            print('Remapping '+nc_name+' to '+target_name)
            print('Started at date and time '+str(datetime.now()))
            with nc4.Dataset(target_name, "w", format="NETCDF4") as ncid: # creating the NetCDF file
                # define the dimensions
                dimid_N = ncid.createDimension(self.remapped_dim_id, len(hruID_var))  # limited dimensiton equal the number of hruID
                dimid_T = ncid.createDimension('time', None)   # unlimited dimensiton
                # Variable time
                time_varid = ncid.createVariable('time', time_dtype_code, ('time', ))
                # Attributes
                time_varid.long_name = self.var_time
                time_varid.units = time_unit  # e.g. 'days since 2000-01-01 00:00' should change accordingly
                time_varid.calendar = time_cal
                time_varid.standard_name = self.var_time
                time_varid.axis = 'T'
                time_varid[:] = time_var
                # Variables lat, lon, subbasin_ID
                lat_varid = ncid.createVariable(self.remapped_var_lat, 'f8', (self.remapped_dim_id, ))
                lon_varid = ncid.createVariable(self.remapped_var_lon, 'f8', (self.remapped_dim_id, ))
                hruId_varid = ncid.createVariable(self.remapped_var_id, 'f8', (self.remapped_dim_id, ))
                # Attributes
                lat_varid.long_name = self.remapped_var_lat
                lon_varid.long_name = self.remapped_var_lon
                hruId_varid.long_name = 'subbasin ID'
                lat_varid.units = 'degrees_north'
                lon_varid.units = 'degrees_east'
                hruId_varid.units = '1'
                lat_varid.standard_name = self.remapped_var_lat
                lon_varid.standard_name = self.remapped_var_lon
                lat_varid[:] = hruID_lat
                lon_varid[:] = hruID_lon
                hruId_varid[:] = hruID_var
                # general attributes for NetCDF file
                ncid.Conventions = 'CF-1.6'
                ncid.Author = 'The data were written by ' + self.author_name
                ncid.License = self.license
                ncid.History = 'Created ' + time.ctime(time.time())
                ncid.Source = 'Case: ' +self.case_name + '; remapped by script from library of Shervan Gharari (https://github.com/ShervanGharari/candex).'
                # write varibales
                for i in np.arange(len(self.var_names)):
                    var_value  = self.__weighted_average( nc_name,
                                                          target_date_times,
                                                          self.var_names[i],
                                                          remap)
                    # Variables writing
                    varid = ncid.createVariable(self.var_names_remapped[i], self.format_list[i], ('time',self.remapped_dim_id ), fill_value = self.fill_value_list[i])
                    varid [:] = var_value
                    # Pass attributes
                    if 'long_name' in ncids.variables[self.var_names[i]].ncattrs():
                        varid.long_name = ncids.variables[self.var_names[i]].long_name
                    if 'units' in ncids.variables[self.var_names[i]].ncattrs():
                        varid.units = ncids.variables[self.var_names[i]].units
                # reporting
                print('Ended   at date and time '+str(datetime.now()))
                print('------')
            if self.save_csv:
                ds = xr.open_dataset(target_name)
                for i in np.arange(len(self.var_names_remapped)):
                    new_list = list(self.var_names_remapped) # new lists
                    del new_list[i] # remove one value
                    ds_temp = ds.drop(new_list) # drop all the other varibales excpet target varibale, lat, lon and time
                    if 'units' in ds[self.var_names_remapped[i]].attrs.keys():
                        dictionary = {self.var_names_remapped[i]:self.var_names_remapped[i]+' ['+ds[self.var_names_remapped[i]].attrs['units']+']'}
                        ds_temp = ds_temp.rename_vars(dictionary)
                    target_name_csv = self.output_dir + self.case_name + '_remapped_'+ self.var_names_remapped[i] +\
                     '_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.csv'
                    if os.path.exists(target_name_csv): # remove file if exists
                        os.remove(target_name_csv)
                    ds_temp = ds_temp.set_coords([self.remapped_var_lat,self.remapped_var_lon])
                    df = ds_temp.to_dataframe()
                    df['ID'] = df.index.get_level_values(level=0)
                    df['time'] = df.index.get_level_values(level=1)
                    df = df.set_index(['ID','time',self.remapped_var_lat,self.remapped_var_lon])
                    df = df.unstack(level=-3)
                    df = df.transpose()
                    if 'units' in ds[self.var_names_remapped[i]].attrs.keys():
                        df = df.replace(self.var_names_remapped[i], self.var_names_remapped[i]+' '+ds[self.var_names_remapped[i]].attrs['units'])
                    df.to_csv(target_name_csv)
                    print('Converting variable '+ self.var_names_remapped[i] +' from remapped file of '+target_name+\
                        ' to '+target_name_csv)
                print('------')

    def __weighted_average(self,
                           nc_name,
                           target_time,
                           varibale_name,
                           mapping_df):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function reads the data for a given time and calculates the weighted average
        Arguments
        ---------
        nc_name: string, name of the netCDF file
        target_time: string,
        varibale_name: string, name of varibale from source netcsf file to be remapped
        mapping_df: pandas dataframe, including the row and column of the source data and weight
        Returns
        -------
        weighted_value: a numpy array that has the remapped values from the nc file
        """
        # open dataset
        ds = xr.open_dataset(nc_name)
        # rename time varibale to time
        if self.var_time != 'time':
            ds = ds.rename({self.var_time:'time'})
        # prepared the numpy array for ouptut
        weighted_value = np.zeros([self.length_of_time,self.number_of_target_elements])
        m = 0 # counter
        for date in target_time: # loop over time
            ds_temp = ds.sel(time=date.strftime("%Y-%m-%d %H:%M:%S"),method="nearest")
            data = np.array(ds_temp[varibale_name])
            data = np.squeeze(data)
            # get values from the rows and cols and pass to np data array
            if self.case ==1 or self.case ==2:
                values = data [self.rows,self.cols]
            if self.case ==3:
                values = data [self.rows]
            values = np.array(values)
            # add values to data frame, weighted average and pass to data frame again
            mapping_df['values'] = values
            mapping_df['values_w'] = mapping_df['weight']*mapping_df['values']
            df_temp = mapping_df.groupby(['ID_t'],as_index=False).agg({'values_w': 'sum'})
            df_temp = df_temp.sort_values(by=['ID_t'])
            weighted_value [m,:] = np.array(df_temp['values_w'])
            m += 1
        return weighted_value

#
#
#
#
#
#
#
#
#
#
#
#


    def make_shape_point(   self,
                            dataframe,
                            lon_field,
                            lat_field):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a geopandas dataframe of lat, lon and IDs provided
        Arguments
        ---------
        lon_d: numpy array, the longitude
        lat_d: numpy array, the latitude
        ID: numpy array, the ID
        Returns
        -------
        df: geopandas dataframe, with geometry of longitude and latitude
        """
        # read the pandas data frame of the all statiosn
        #from   shapely.geometry import Point
        import geopandas as gpd
        import pandas as pd
        #df['geometry']  = df.apply(lambda row: Point(row.LONGITUDE, row.LATITUDE ), axis=1) # set the geometry
        # shp  = gpd.GeoDataFrame(df) # pass this to a geopandas dataframe
        shp = gpd.GeoDataFrame(dataframe, geometry=gpd.points_from_xy(df.lon, df.lat))
        return shp

    # def lat_lon_value_geotiff(  self,
    #                             geotiff_name,
    #                             make_shapefile=False):

    #     from pysheds.grid import Grid
    #     import pandas as pd

    #     grid = Grid.from_raster(geotiff_name, data_name='temp') # part of Missouri River
    #     dem_coords = grid.temp.coords
    #     lat = np.array(dem_coords[:,0].reshape(grid.temp.shape)).flatten()
    #     lon = np.array(dem_coords[:,1].reshape(grid.temp.shape)).flatten()
    #     values = np.array(grid.temp).flatten()
    #     df = pd.DataFrame()
    #     df['lat']  = lat
    #     df['lon'] = lon
    #     df['value']  = values
    #     print(type(df))
    #     if make_shapefile:
    #         shp = self.make_shape_point (df, 'lon', 'lat')
    #     else:
    #         shp = None
    #     return df, shp

    def bbox_to_pixel_offsets(self,gt, bbox):
        """
        Zonal Statistics
        Vector-Raster Analysis
        Copyright 2013 Matthew Perry
        Usage:
          zonal_stats.py VECTOR RASTER
          zonal_stats.py -h | --help
          zonal_stats.py --version
        Options:
          -h --help     Show this screen.
          --version     Show version.
        """
        originX = gt[0]
        originY = gt[3]
        pixel_width = gt[1]
        pixel_height = gt[5]
        x1 = int((bbox[0] - originX) / pixel_width)
        x2 = int((bbox[1] - originX) / pixel_width) + 1
        y1 = int((bbox[3] - originY) / pixel_height)
        y2 = int((bbox[2] - originY) / pixel_height) + 1
        xsize = x2 - x1
        ysize = y2 - y1
        return (x1, y1, xsize, ysize)

    def zonal_stat(self, vector_path, raster_path, band =1, nodata_value=None, global_src_extent=False):
        from osgeo import gdal, ogr
        # from osgeo.gdalconst import *
        import osgeo.gdalconst
        import numpy as np
        import sys
        gdal.PushErrorHandler('CPLQuietErrorHandler')
        import pandas as pd
        import os
        import geopandas as gpd
        """
        original code:
        Zonal Statistics
        Vector-Raster Analysis
        Copyright 2013 Matthew Perry
        Usage:
          zonal_stats.py VECTOR RASTER
          zonal_stats.py -h | --help
          zonal_stats.py --version
        Options:
          -h --help     Show this screen.
          --version     Show version.
        changes by:
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a geopandas dataframe of lat, lon and IDs provided
        Arguments
        ---------
        vector_path: string; the path to the shapefile
        raster_path: string; the path to the raster
        band: the band in the raster
        nodata_value: set as None
        global_src_extent : set as False
        Returns
        -------
        shp: geodataframe, with values of mean, max, min, sum, std, count, fid
        """
        # read the ratster file band number n
        rds = gdal.Open(raster_path, osgeo.gdalconst.GA_ReadOnly)
        assert(rds)
        rb = rds.GetRasterBand(band)
        rgt = rds.GetGeoTransform()
        # if nodata_value is identified
        if nodata_value:
            nodata_value = float(nodata_value)
            rb.SetNoDataValue(nodata_value)
        # read vector data
        vds = ogr.Open(vector_path, osgeo.gdalconst.GA_ReadOnly)  # TODO maybe open update if we want to write stats
        assert(vds)
        vlyr = vds.GetLayer(0)
        # create an in-memory numpy array of the source raster data
        # covering the whole extent of the vector layer
        if global_src_extent:
            # use global source extent
            # useful only when disk IO or raster scanning inefficiencies are your limiting factor
            # advantage: reads raster data in one pass
            # disadvantage: large vector extents may have big memory requirements
            src_offset = self.bbox_to_pixel_offsets(rgt, vlyr.GetExtent())
            src_array = rb.ReadAsArray(*src_offset)
            # calculate new geotransform of the layer subset
            new_gt = (
                (rgt[0] + (src_offset[0] * rgt[1])),
                rgt[1],
                0.0,
                (rgt[3] + (src_offset[1] * rgt[5])),
                0.0,
                rgt[5]
            )
        mem_drv = ogr.GetDriverByName('Memory')
        driver = gdal.GetDriverByName('MEM')
        # Loop through vectors
        stats = []
        feat = vlyr.GetNextFeature()
        while feat is not None:
            if not global_src_extent:
                # use local source extent
                # fastest option when you have fast disks and well indexed raster (ie tiled Geotiff)
                # advantage: each feature uses the smallest raster chunk
                # disadvantage: lots of reads on the source raster
                src_offset = self.bbox_to_pixel_offsets(rgt, feat.geometry().GetEnvelope())
                src_array = rb.ReadAsArray(*src_offset)
                # calculate new geotransform of the feature subset
                new_gt = (
                    (rgt[0] + (src_offset[0] * rgt[1])),
                    rgt[1],
                    0.0,
                    (rgt[3] + (src_offset[1] * rgt[5])),
                    0.0,
                    rgt[5]
                )
            if src_array is not None:
                # Create a temporary vector layer in memory
                mem_ds = mem_drv.CreateDataSource('out')
                mem_layer = mem_ds.CreateLayer('poly', None, ogr.wkbPolygon)
                mem_layer.CreateFeature(feat.Clone())
                # Rasterize it
                rvds = driver.Create('', src_offset[2], src_offset[3], 1, gdal.GDT_Byte)
                rvds.SetGeoTransform(new_gt)
                gdal.RasterizeLayer(rvds, [1], mem_layer, burn_values=[1])
                rv_array = rvds.ReadAsArray()
                # Mask the source data array with our current feature
                # we take the logical_not to flip 0<->1 to get the correct mask effect
                # we also mask out nodata values explictly
                masked = np.ma.MaskedArray(
                    src_array,
                    mask=np.logical_or(
                        src_array == nodata_value,
                        np.logical_not(rv_array)
                    )
                )
                feature_stats = {
                    'min': float(masked.min()),
                    'mean': float(masked.mean()),
                    'max': float(masked.max()),
                    'std': float(masked.std()),
                    'sum': float(masked.sum()),
                    'count': int(masked.count()),
                    'fid': int(feat.GetFID())}
            else:
                feature_stats = {
                    'min': 'NaN',
                    'mean': 'NaN',
                    'max': 'NaN',
                    'std': 'NaN',
                    'sum': 'NaN',
                    'count': 'NaN',
                    'fid': 'NaN'}
            stats.append(feature_stats)
            rvds = None
            mem_ds = None
            feat = vlyr.GetNextFeature()
        vds = None
        rds = None
        stats = pd.DataFrame(stats)
        shp = gpd.read_file(vector_path)
        shp ['min'] = stats['min'].astype(float)
        shp ['mean'] = stats['mean'].astype(float)
        shp ['max'] = stats['max'].astype(float)
        shp ['std'] = stats['std'].astype(float)
        shp ['sum'] = stats['sum'].astype(float)
        shp ['count'] = stats['count'].astype(float)
        shp ['fid'] =  stats['fid'].astype(float)
        return shp

    def geotiff_zones(self, raster_path_in, raster_path_out, band =1, num_bin=10, slice_level = None):
        from osgeo import gdal, ogr
        # from osgeo.gdalconst import *
        import osgeo.gdalconst
        import numpy as np
        import sys
        gdal.PushErrorHandler('CPLQuietErrorHandler')
        import pandas as pd
        import os
        import geopandas as gpd
        """
        original code:
        Zonal Statistics
        Vector-Raster Analysis
        Copyright 2013 Matthew Perry
        Usage:
          zonal_stats.py VECTOR RASTER
          zonal_stats.py -h | --help
          zonal_stats.py --version
        Options:
          -h --help     Show this screen.
          --version     Show version.
        changes by:
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a geopandas dataframe of lat, lon and IDs provided
        Arguments
        ---------
        vector_path: string; the path to the shapefile
        raster_path: string; the path to the raster
        band: the band in the raster
        nodata_value: set as None
        global_src_extent : set as False
        Returns
        -------
        shp: geodataframe, with values of mean, max, min, sum, std, count, fid
        """
        # read the ratster file band number n
        import os
        import gdal
        import numpy as np
        import matplotlib.pyplot as plt
        ds = gdal.Open(raster_path_in)
        band = ds.GetRasterBand(band)
        arr = band.ReadAsArray()
        [cols, rows] = arr.shape
        arr_min = arr.min()
        arr_max = arr.max()
        print(arr_min, arr_max)
        if slice_level is None: # calculate the delta from min to max with number of bins
            delta = np.arange(arr_min, arr_max, (arr_max-arr_min)/num_bin)
        else:
            # check if slice_level is monotonically increasing
            slice_level = slice_level.flatten()
            if slice_level.ndim != 1:
                sys.exit('it seems the provided slice levels are not 1 dimentional numpy array')
            if len(slice_level) != len(np.unique(slice_level)):
                sys.exit('it seems the provided slice levels do not have unique values')
            if not np.all(np.diff(slice_level) > 0):
                sys.exit('it seems the provided slice levels are not stricktly increasing')
            delta = slice_level
            # check values:
            if (arr_min > delta.max()) or (arr_max < delta.min()):
                print('max from geotiff: ', arr_max, 'min from geotiff: ', arr_min)
                sys.exit('it seems the provided slice levels are not stricktly increasing')
        arr_out = arr
        for i in np.arange(len(delta)-1):
            arr_out = np.where(np.logical_and(arr_out>=delta[i], arr_out<=delta[i+1]), (delta[i]+delta[i+1])/2,arr_out)
        # saving
        driver = gdal.GetDriverByName("GTiff")
        outdata = driver.Create(raster_path_out, rows, cols, 1, gdal.GDT_UInt16)
        outdata.SetGeoTransform(ds.GetGeoTransform())##sets same geotransform as input
        outdata.SetProjection(ds.GetProjection())##sets same projection as input
        outdata.GetRasterBand(1).WriteArray(arr_out)
        outdata.GetRasterBand(1).SetNoDataValue(0)##if you want these values transparent
        outdata.FlushCache() ##saves to disk!!
        outdata = None
        band=None
        ds=None


    def geotiff2shp(self, raster_path_in, vector_path_out, band = 1):

        from osgeo import gdal, ogr
        import sys
        # this allows GDAL to throw Python Exceptions
        #gdal.UseExceptions()
        src_ds = gdal.Open(raster_path_in)
        srcband = src_ds.GetRasterBand(1)
        drv = ogr.GetDriverByName('ESRI Shapefile')
        dst_ds = drv.CreateDataSource(vector_path_out)
        dst_layer = dst_ds.CreateLayer(vector_path_out , srs=None)
        fd = ogr.FieldDefn('DN', ogr.OFTInteger)
        dst_layer.CreateField(fd)
        dst_field = dst_layer.GetLayerDefn().GetFieldIndex('DN')
        gdal.Polygonize(srcband, None, dst_layer, dst_field, [], callback=None)
