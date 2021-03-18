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
        self.sink_shp                  =  '' # sink shapefile
        self.sink_shp_ID               =  '' # name_of_field_target_shp
        self.sink_shp_lat              =  '' # name_of_field_target_shp
        self.sink_shp_lon              =  '' # name_of_field_target_shp
        self.source_nc                 =  '' # name of nc file to be remapped
        self.var_names                 =  [] # list of varibale names to be remapped
        self.var_lon                   =  '' # name of varibale longitude
        self.var_lat                   =  '' # name of varibale latitude
        self.var_time                  =  '' # name of varibale time
        self.var_ID                    =  '' # name of varibale ID
        self.source_shp                =  '' # name of source shapefile
        self.source_shp_lat            =  '' # name of latitude field in source shapefile
        self.source_shp_lon            =  '' # name of longitude field in source shapefile
        self.source_shp_ID             =  '' # name of ID filed in source shapefile
        self.temp_dir                  =  './temp/' # temp_dir
        self.output_dir                =  '' # source_nc
        self.format_list               =  []
        self.fill_value_list           =  []
        self.remap_csv                 =  '' # source_nc
        self.authour_name              =  ''
        self.tolerance                 =  10**-5 # tolerance
        self.get_col_row_flag          =  False
        self.save_csv                  =  False
        # self.box_flag                  =  True # box_flag; may not be used...
        # self.map_on_ID                 =  False # for future development for remapping on IDs only not supported and not recommended


    def run_candex(self):

        # check candex input
        self.check_candex_input()

        # if remap is not provided then create the remapping file
        if self.remap_csv == '':

            import geopandas as gpd

            # check the target shapefile
            sink_shp_gpd = gpd.read_file(self.sink_shp)
            sink_shp_gpd = self.check_target_shp(sink_shp_gpd)
            # save the standard target shapefile
            print('candex will save standard shapefile for candex claculation as:')
            print(self.temp_dir+self.case_name+'_sink_shapefile.shp')
            sink_shp_gpd.to_file(self.temp_dir+self.case_name+'_sink_shapefile.shp') # save

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
            shp_1 = gpd.read_file(self.temp_dir+self.case_name+'_sink_shapefile.shp')
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
                os.remove(self.temp_dir+self.case_name+'test.*')
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

    def check_candex_input (self):
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
        if self.authour_name == '':
            print("no  author name is provide and the author name is changes to (author name)!")
            self.authour_name = "author name"
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


    def check_target_shp (self,shp):

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
        if self.sink_shp_ID == '':
            print('candex detects that no field for ID is provided in sink/target shapefile')
            print('arbitarary values of ID are added in the field ID_t')
            shp['ID_t']  = np.arange(len(shp))+1
        else:
            print('candex detects that the field for ID is provided in sink/target shapefile')
            # check if the provided IDs are unique
            ID_values = np.array(shp[self.sink_shp_ID])
            if len(ID_values) != len(np.unique(ID_values)):
                sys.exit('The provided IDs in shapefile are not unique; provide unique IDs or do not identify sink_shp_ID')
            shp['ID_t'] = shp[self.sink_shp_ID]
        if self.sink_shp_lat == '':
            print('candex detects that no field for latitude is provided in sink/target shapefile')
            print('latitude values are added in the field lat_t')
            shp['lat_t']  = shp.centroid.y # centroid lat from target
        else:
            print('candex detects that the field latitude is provided in sink/target shapefile')
            shp['lat_t'] = shp[self.sink_shp_lat]
        if self.sink_shp_lon == '':
            print('candex detects that no field for longitude is provided in sink/target shapefile')
            print('longitude values are added in the field lon_t')
            shp['lon_t']  = shp.centroid.x # centroid lon from target
        else:
            print('candex detects that the field longitude is provided in sink/target shapefile')
            shp['lon_t'] = shp[self.sink_shp_lon]
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
        print('it seems everything is OK with the sink/target shapefile; added to candex object sink_shp_gpd')
        return shp

    def check_source_nc (self):
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

        # load the needed packages
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        #
        multi_source = False
        #
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
        @ license:                  Apache2

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
            print(lat_temp_diff_unique)
            print(lat_temp_diff_unique.shape)
            #
            lon_temp = np.array(ncid.variables[self.var_lon][:])
            lon_temp_diff = np.diff(lon_temp)
            lon_temp_diff_unique = np.unique(lon_temp_diff)
            print(lon_temp_diff_unique)
            print(lon_temp_diff_unique.shape)
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
            print(lat, lon)
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

    def lat_lon_SHP(self, lat, lon, file_name):
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
        # check if lat/lon that are taken in has the same dimension
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely

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

    def add_lat_lon_source_SHP(self, shp, source_shp_lat, source_shp_lon, source_shp_ID):
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

    def expand_source_SHP(self, shp, temp_dir, case_name):
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

    def intersection_shp(self, shp_1, shp_2):
        import geopandas as gpd
        from   shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
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
        shp_1: geo data frame, shapefile 1
        shp_2: geo data frame, shapefile 2

        Returns
        -------
        result: a geo data frame that includes the intersected shapefile and area, percent and normalized percent of each shape
        elements in another one
        """
        # Calculating the area of every shapefile (both should be in degree or meters)

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

    def spatial_overlays(self, df1, df2, how='intersection', reproject=True):
        import geopandas as gpd
        from shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
        """
        Perform spatial overlay between two polygons.
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

    def create_remap(self, int_df, lat_source, lon_source):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/candex
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 Apache2
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


    def create_row_col_df (self, lat_source, lon_source, lat_target_int, lon_target_int):

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


    def check_candex_remap(self, remap_df):

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
            if 'units' in ncids.variables[self.var_time].ncattrs():
                time_unit = ncids.variables[self.var_time].units
            if 'calendar' in ncids.variables[self.var_time].ncattrs():
                time_cal = ncids.variables[self.var_time].calendar
            time_var = ncids[self.var_time][:]
            self.length_of_time = len(time_var)
            target_date_times = nc4.num2date(time_var,units = time_unit,calendar = time_cal)
            target_name = self.output_dir + self.case_name + '_remapped_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.nc'
            if os.path.exists(target_name): # remove file if exists
                os.remove(target_name)

            # reporting
            print('Remapping '+nc_name+' to '+target_name)
            print('Started at date and time '+str(datetime.now()))

            with nc4.Dataset(target_name, "w", format="NETCDF4") as ncid: # creating the NetCDF file

                # define the dimensions
                dimid_N = ncid.createDimension('ID', len(hruID_var))  # limited dimensiton equal the number of hruID
                dimid_T = ncid.createDimension('time', None)   # unlimited dimensiton

                # Variable time
                time_varid = ncid.createVariable('time', 'i4', ('time', ))
                # Attributes
                time_varid.long_name = self.var_time
                time_varid.units = time_unit  # e.g. 'days since 2000-01-01 00:00' should change accordingly
                time_varid.calendar = time_cal
                time_varid.standard_name = self.var_time
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
                ncid.License = 'The data were written by ' + self.authour_name
                ncid.history = 'Created ' + time.ctime(time.time())
                ncid.source = 'Case: ' +self.case_name + '; remapped by script from library of Shervan Gharari (https://github.com/ShervanGharari/candex).'


                # write varibales
                for i in np.arange(len(self.var_names)):

                    var_value  = self.__weighted_average( nc_name,
                                                          target_date_times,
                                                          self.var_names[i],
                                                          remap)

                    # Variables writing
                    varid = ncid.createVariable(self.var_names[i], self.format_list[i], ('time','ID' ), fill_value = self.fill_value_list[i])
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
                for i in np.arange(len(self.var_names)):
                    new_list = list(self.var_names) # new lists
                    del new_list[i] # remove one value
                    ds_temp = ds.drop(new_list) # drop all the other varibales excpet target varibale, lat, lon and time
                    if 'units' in ds[self.var_names[i]].attrs.keys():
                        dictionary = {self.var_names[i]:self.var_names[i]+' ['+ds[self.var_names[i]].attrs['units']+']'}
                        ds_temp = ds_temp.rename_vars(dictionary)
                    target_name_csv = self.output_dir + self.case_name + '_remapped_'+ self.var_names[i] +\
                     '_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.csv'
                    if os.path.exists(target_name_csv): # remove file if exists
                        os.remove(target_name_csv)
                    ds_temp = ds_temp.set_coords([self.var_lat,self.var_lon])
                    df = ds_temp.to_dataframe()
                    df['ID'] = df.index.get_level_values(level=0)
                    df['time'] = df.index.get_level_values(level=1)
                    df = df.set_index(['ID','time',self.var_lat, self.var_lon])
                    df = df.unstack(level=-3)
                    df = df.transpose()
                    if 'units' in ds[self.var_names[i]].attrs.keys():
                        print('in')
                        df = df.replace(self.var_names[i], self.var_names[i]+' '+ds[self.var_names[i]].attrs['units'])
                    df.to_csv(target_name_csv)
                    print('Converting variable '+ self.var_names[i] +' from remapped file of '+target_name+\
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

        # open dataset
        ds = xr.open_dataset(nc_name)

        # rename time varibale to time
        ds = ds.rename({self.var_time:'time'})

        # prepared the numpy array for ouptut
        weighted_value = np.zeros([self.length_of_time,self.number_of_target_elements])
        m = 0 # counter

        for date in target_time: # loop over time

            ds_temp = ds.sel(time=date.strftime("%Y-%m-%d %H:%M:%S"),method="nearest")
            data = np.array(ds_temp[varibale_name])

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

            m = m+1

        return weighted_value
