# section 1 load all the necessary modules and packages

import glob
import time
import netCDF4      as nc4
import numpy        as np
import pandas       as pd
import xarray       as xr
import sys
import os
import warnings
from   datetime     import datetime
import re
import json


class easymore:

    def __init__(self):
        self.delet_attr()                 # remove the existing variables
        self.case_name                 =  'case_temp' # name of the case
        self.target_shp                =  '' # sink/target shapefile
        self.target_shp_ID             =  '' # name of the column ID in the sink/target shapefile
        self.target_shp_lat            =  '' # name of the column latitude in the sink/target shapefile
        self.target_shp_lon            =  '' # name of the column longitude in the sink/target shapefile
        self.source_nc                 =  '' # name of nc file to be remapped
        self.var_names                 =  [] # list of variable names to be remapped from the source NetCDF file
        self.var_lon                   =  '' # name of variable longitude in the source NetCDF file
        self.var_lat                   =  '' # name of variable latitude in the source NetCDF file
        self.var_time                  =  'time' # name of variable time in the source NetCDF file
        self.var_ID                    =  '' # name of variable ID in the source NetCDF file
        self.var_station               =  '' # name of variable station in the source NetCDF file
        self.var_names_remapped        =  [] # list of variable names that will be replaced in the remapped file
        self.skip_check_all_source_nc  =  False # if set to True only first file will be check for variables and dimensions and not the all the files (recommneded not to change)
        self.source_shp                =  '' # name of source shapefile (essential for case-3)
        self.source_shp_lat            =  '' # name of column latitude in the source shapefile
        self.source_shp_lon            =  '' # name of column longitude in the source shapefile
        self.source_shp_ID             =  '' # name of column ID in the source shapefile
        self.remapped_var_id           =  'ID' # name of the ID variable in the new nc file; default 'ID'
        self.remapped_var_lat          =  'latitude' # name of the latitude variable in the new nc file; default 'latitude'
        self.remapped_var_lon          =  'longitude' # name of the longitude variable in the new nc file; default 'longitude'
        self.remapped_dim_id           =  'ID' # name of the ID dimension in the new nc file; default 'ID'
        self.remapped_chunk_size       =  200 # chunksize of remapped variables in the non-time (i.e. limited) dimension. Default 200. Use 'None' for netCDF4 defaults
        self.overwrite_remapped_nc     =  True # Flag to automatically overwrite existing remapping files. If 'False', aborts the remapping procedure if a file is detected
        self.temp_dir                  =  './temp/' # temp_dir
        self.output_dir                =  './output/' # output directory
        self.format_list               =  ['f8'] # float for the remapped values
        self.fill_value_list           =  ['-9999'] # missing values set to -9999
        self.remap_csv                 =  '' # name of the remapped file if provided
        self.only_create_remap_csv     =  False # is true it does not remap any nc files
        self.clip_source_shp           =  True # the source shapefile is clipped to the domain of target shapefile to increase intersection speed
        self.buffer_clip_source_shp    =  2  # 2 degrees for buffer to clip the source shapefile based on target shapefile
        self.save_temp_shp             =  True # if set to false does not save the temporary shapefile in the temp folder for large shapefiles
        self.correction_shp_lon        =  True # correct for -180 to 180 and 0 to 360 longitude
        self.rescaledweights           =  True # if set true the weights are rescaled
        self.skip_outside_shape        =  False # if set to True it will not carry the nan values for shapes that are outside the source netCDF geographical domain
        self.author_name               =  '' # name of the authour
        self.license                   =  '' # data license
        self.tolerance                 =  10**-5 # tolerance
        self.save_csv                  =  False # save csv
        self.sort_ID                   =  False # to sort the remapped based on the target shapfile ID; self.target_shp_ID should be given
        self.complevel                 =  4 # netcdf compression level from 1 to 9. Any other value or object will mean no compression.
        self.version                   =  '1.0.0' # version of the easymore
        print('EASYMORE version '+self.version+ ' is initiated.')


    ##############################################################
    #### Configuration from json file
    ##############################################################

    def delet_attr(self):
        # remove attributes that are not function from the easymore object for initialization
        for label in vars(self).keys():
            delattr(self, l)
            print('deleted ', l)

    def read_config_dict (self,
                          config_file_name):

        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function read a json file
        config_file_name: name of the
        """
        # reading the data from the file
        with open(config_file_name) as f:
            data = f.read()
        # reconstructing the data as a json
        config_dict = json.loads(data)
        # return
        return config_dict

    def init_from_dict (self,
                        dict_name):

        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function take a dict_name and populate the variables needed to be initialized for EASYMORE
        """
        if isinstance(dict_name, str): # read the file is string is provided
            dictionary = self.read_config_dict(dict_name)
        elif isinstance(dict_name, dict): # pass the dictionary
            dictionary = dict_name
        else:
            sys.exit('configuration dictionary should be a file name [string], or a dictionary')
        # loop over the dictionary keys
        for key in list(dictionary.keys()):
            if key in list(vars(self).keys()):
                setattr(self, key, dictionary[key])
            else:
                sys.exit('provided key in configuration dictionary or file is not recongnized by easymore keys')


    ##############################################################
    #### NetCDF remapping
    ##############################################################

    def nc_remapper(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function runs a set of EASYMORE function which can remap variable(s) from a srouce shapefile
        with regular, roated, irregular to a target shapefile
        """
        # check EASYMORE input
        self.check_easymore_input()
        # check the source nc file
        self.check_source_nc()
        # if remap is not provided then create the remapping file
        if self.remap_csv == '':
            import geopandas as gpd
            print('--CREATING-REMAPPING-FILE--')
            time_start = datetime.now()
            print('Started at date and time ' + str(time_start))
            # read and check the target shapefile
            target_shp_gpd = gpd.read_file(self.target_shp)
            target_shp_gpd = self.check_target_shp(target_shp_gpd)
            # save the standard target shapefile
            if self.save_temp_shp:
                target_shp_gpd.to_file(self.temp_dir+self.case_name+'_target_shapefile.shp') # save
                print('EASYMORE saved target shapefile for EASYMORE claculation as:')
                print(self.temp_dir+self.case_name+'_target_shapefile.shp')
            # create source shapefile
            source_shp_gpd = self.create_source_shp()
            if self.save_temp_shp:
                source_shp_gpd.to_file(self.temp_dir+self.case_name+'_source_shapefile.shp')
                print(self.temp_dir+self.case_name+'_source_shapefile.shp')
                print('EASYMORE created the shapefile from the netCDF file and saved it here:')
            # intersection of the source and sink/target shapefile
            if self.save_temp_shp:
                shp_1 = gpd.read_file(self.temp_dir+self.case_name+'_target_shapefile.shp')
                shp_2 = gpd.read_file(self.temp_dir+self.case_name+'_source_shapefile.shp')
            else:
                shp_1 = target_shp_gpd
                shp_2 = source_shp_gpd
            # correction of the source and target shapefile to frame of -180 to 180
            min_lon_t, min_lat_t, max_lon_t, max_lat_t = shp_1.total_bounds # target
            min_lon_s, min_lat_s, max_lon_s, max_lat_s = shp_2.total_bounds # source
            if not ((min_lon_s<min_lon_t) and (max_lon_s>max_lon_t) and \
                    (min_lat_s<min_lat_t) and (max_lat_s>max_lat_t)): # heck if taret is not in source
                print('EASMORE detects that target shapefile is outside the boundary of source netCDF file ',
                      'and therefore correction for longitude values -180 to 180 or 0 to 360 if correction_shp_lon ',
                      'flag is set to True [default is True]')
                if self.correction_shp_lon:
                    shp_1 = self.shp_lon_correction(shp_1)
                    shp_2 = self.shp_lon_correction(shp_2)
            else: # it target is in source
                print('EASMORE detects that target shapefile is inside the boundary of source netCDF file ',
                      'and therefore correction for longitude values -180 to 180 or 0 to 360 is not performed even if ',
                      'the correction_shp_lon flag is set to True [default is True]')
            if self.save_temp_shp:
                shp_1.to_file(self.temp_dir+self.case_name+'_target_shapefile_corrected_frame.shp')
                shp_2.to_file(self.temp_dir+self.case_name+'_source_shapefile_corrected_frame.shp')
            # # clip to the region of the target shapefile to speed up the intersection
            # if self.clip_source_shp:
            #     min_lon, min_lat, max_lon, max_lat = shp_1.total_bounds
            #     min_lon, min_lat, max_lon, max_lat = min_lon - self.buffer_clip_source_shp, min_lat - self.buffer_clip_source_shp,\
            #                                          max_lon + self.buffer_clip_source_shp, max_lat + self.buffer_clip_source_shp
            #     shp_2 = shp_2[(shp_2['lat_s']<max_lat) & (shp_2['lat_s']>min_lat) & (shp_2['lon_s']<max_lon) & (shp_2['lon_s']>min_lon)]
            #     shp_2.reset_index(drop=True, inplace=True)
            #     if self.save_temp_shp:
            #         if self.correction_shp_lon:
            #             shp_2.to_file(self.temp_dir+self.case_name+'_source_shapefile_corrected_frame_clipped.shp')
            #         else:
            #             shp_2.to_file(self.temp_dir+self.case_name+'_source_shapefile_clipped.shp')
            # reprojections to equal area
            if self.check_shp_crs(shp_1) and self.check_shp_crs(shp_2): #(str(shp_1.crs).lower() == str(shp_2.crs).lower()) and ('epsg:4326' in str(shp_1.crs).lower()):
                shp_1 = shp_1.to_crs ("EPSG:6933") # project to equal area
                shp_2 = shp_2.to_crs ("EPSG:6933") # project to equal area
                if self.save_temp_shp:
                    shp_1.to_file(self.temp_dir+self.case_name+'test.shp')
                    shp_1 = gpd.read_file(self.temp_dir+self.case_name+'test.shp')
                    shp_2.to_file(self.temp_dir+self.case_name+'test.shp')
                    shp_2 = gpd.read_file(self.temp_dir+self.case_name+'test.shp')
                # remove test files
                removeThese = glob.glob(self.temp_dir+self.case_name+'test.*')
                for file in removeThese:
                    os.remove(file)
            else:
                sys.exit('The projection for source and target shapefile are not WGS84, please revise, assign')
            # intersection
            warnings.simplefilter('ignore')
            shp_int = self.intersection_shp(shp_1, shp_2)
            warnings.simplefilter('default')
            shp_int = shp_int.sort_values(by=['S_1_ID_t']) # sort based on ID_t
            shp_int = shp_int.to_crs ("EPSG:4326") # project back to WGS84
            if self.save_temp_shp:
                shp_int.to_file(self.temp_dir+self.case_name+'_intersected_shapefile.shp') # save the intersected files
            shp_int = shp_int.drop(columns=['geometry']) # remove the geometry
            # compare shp_1 or target shapefile with intersection to see if all the shape exists in intersection
            order_values_shp_1   = np.unique(np.array(shp_1['S_1_order']))
            order_values_shp_int = np.unique(np.array(shp_int['S_1_order']))
            diff = np.setdiff1d(order_values_shp_1, order_values_shp_int)
            if (diff.size > 0):
                np.savetxt(self.temp_dir+self.case_name+'ID_not_intersected.txt', diff, fmt='%d')
                print('Warning: There are shapes that are outside the boundaries of the provided netCDF file. The IDs of those'+\
                      'shapes are saved in: \n'+self.temp_dir+self.case_name+'ID_not_intersected.txt')
                if not (self.skip_outside_shape): # not all the elements of target shapefile are in intersection
                    shp_1_not_int = shp_1[shp_1['S_1_order'].isin(diff)]
                    shp_1_not_int = shp_1_not_int.drop(columns=['geometry']) # remove the geometry
                    shp_1_not_int['S_2_lat_s'] = 0.00 # assign random lat, no influence as weight is non existing in shp_int
                    shp_1_not_int['S_2_lon_s'] = 0.00 # assign random lon, no influence as weight if non existing in shp_int
                    shp_int = pd.concat([shp_int, shp_1_not_int],axis=0) # contact the shp_int and shapes that are not intersected
                    print('Warning: There are shapes that are outside the boundaries of the provided netCDF file. Those shapes '+\
                          'this will reduce the speed of remapping of the source to remaped netCDF file '+\
                          'to increase the speed you should make sure that target shapefile is within the boundary of provided '+\
                          'netCDF file or set the easymore flag of skip_outside_shape to True.'+\
                          'this flag ensures the shapes that are outside of the netCDF domain are not transfered as nan to remapped '+\
                          'netCDF files')
            # rename dictionary
            dict_rename = {'S_1_ID_t' : 'ID_t',
                           'S_1_lat_t': 'lat_t',
                           'S_1_lon_t': 'lon_t',
                           'S_1_order': 'order_t',
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
            time_end = datetime.now()
            time_diff = time_end-time_start
            print('Ended at date and time ' + str(time_end))
            print('It took '+ str(time_diff.total_seconds())+' seconds to finish creating of the remapping file')
            print('---------------------------')
        else:
            # check the remap file if provided
            int_df  = pd.read_csv(self.remap_csv)
            self.check_easymore_remap(int_df)
        # check if the remapping file needs to be generated only
        if self.only_create_remap_csv:
            print('The flag to create only remap file is True')
            print('The remapping file is either created or given to EASYMORE')
            print('The remapping Located here: ', self.remap_csv)
        else:
            self.__target_nc_creation()


    def create_source_shp(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a source shapefile for regular or rotated grid and Voronoi diagram from
        source netCDF file
        """
        import geopandas as gpd
        # find the case, 1: regular, 2: rotated, 3: irregular Voronoi diagram creation if not provided
        self.NetCDF_SHP_lat_lon()
        # create the source shapefile for case 1 and 2 if shapefile is not provided
        if (self.case == 1 or self.case == 2):
            if (self.source_shp == ''):
                if self.case == 1 and hasattr(self, 'lat_expanded') and hasattr(self, 'lon_expanded'):
                    source_shp_gpd = self.lat_lon_SHP(self.lat_expanded, self.lon_expanded,crs="epsg:4326")
                else:
                    source_shp_gpd = self.lat_lon_SHP(self.lat, self.lon,crs="epsg:4326")
            else:
                source_shp_gpd = gpd.read_file(self.source_shp)
                source_shp_gpd = self.add_lat_lon_source_SHP(source_shp_gpd, self.source_shp_lat,\
                                                             self.source_shp_lon, self.source_shp_ID)
        # if case 3
        if (self.case == 3):
            if (self.source_shp != ''): # source shapefile is provided
                self.check_source_nc_shp() # check the lat lon in soure shapefile and nc file
                source_shp_gpd = gpd.read_file(self.source_shp)
                source_shp_gpd = self.add_lat_lon_source_SHP(source_shp_gpd, self.source_shp_lat,\
                                                             self.source_shp_lon, self.source_shp_ID)
            if (self.source_shp == ''): # source shapefile is not provided goes for voronoi
                # Create the source shapefile using Voronio diagram
                print('EASYMORE detect that source shapefile is not provided for irregulat lat lon source NetCDF')
                print('EASYMORE will create the voronoi source shapefile based on the lat lon')
                source_shp_gpd, source_shp_point_gpd = self.shp_from_irregular_nc (station_shp_file_name = self.temp_dir+self.case_name+'_source_shapefile_points.shp')
        return source_shp_gpd

    def get_col_row(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates the dataframe with assosiated latitude and longitude or source file and
        its location of data by column and row for EASYMORE to extract/remap data
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

    def check_easymore_input(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        the functions checkes if the necessary EASYMORE object are provided from the user
        """
        if self.temp_dir != '':
            if self.temp_dir[-1] != '/':
                sys.exit('the provided temporary folder for EASYMORE should end with (/)')
            if not os.path.isdir(self.temp_dir):
                os.makedirs(self.temp_dir)
        if self.output_dir == '':
            sys.exit('the provided folder for EASYMORE remapped netCDF output is missing; please provide that')
        if self.output_dir != '':
            if self.output_dir[-1] != '/':
                sys.exit('the provided output folder for EASYMORE should end with (/)')
            if not os.path.isdir(self.output_dir):
                os.makedirs(self.output_dir)
        if self.temp_dir == '':
            print("No temporary folder is provided for EASYMORE; this will result in EASYMORE saving the files in the same directory as python script")
        if self.author_name == '':
            print("no author name is provided. The author name is changed to (author name)!")
            self.author_name = "author name"
        if (len(self.var_names) != 1) and (len(self.format_list) == 1) and (len(self.fill_value_list) ==1):
            if (len(self.var_names) != len(self.fill_value_list)) and \
            (len(self.var_names) != len(self.format_list)) and \
            (len(self.format_list) == 1) and (len(self.fill_value_list) ==1):
                print('EASYMORE is given multiple variables for remapping but only on format and fill value. '+\
                      'EASYMORE repeats the format and fill value for all the variables in output files')
                self.format_list     = self.format_list     * len(self.var_names)
                self.fill_value_list = self.fill_value_list * len(self.var_names)
            else:
                sys.exit('number of variables and fill values and formats do not match')
        if self.remap_csv != '':
            print('remap file is provided; EASYMORE will use this file and skip creation of remapping file')
        if len(self.var_names) != len(set(self.var_names)):
            sys.exit('names of variables provided from the source NetCDF file to be remapped are not unique')
        if self.var_names_remapped:
            if len(self.var_names_remapped) != len(set(self.var_names_remapped)):
                sys.exit('the name of the variables you have provided as the rename in the remapped file are not unique')
            if len(self.var_names_remapped) != len(self.var_names):
                sys.exit('the number of provided variables from the source file and names to be remapped are not the same length')
        else:
            self.var_names_remapped = self.var_names
        for i in np.arange(len(self.var_names)):
            print('EASYMORE will remap variable ',self.var_names[i],' from source file to variable ',self.var_names_remapped[i],' in remapped netCDF file')

    def check_target_shp (self,shp):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
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
        #import shapefile # pyshed library
        import shapely
        # sink/target shapefile check the projection
        if not self.check_shp_crs(shp):
            sys.exit('please project your shapefile to WGS84 (epsg:4326)')
        else: # check if the projection is WGS84 (or epsg:4326)
            print('EASYMORE detects that target shapefile is in WGS84 (epsg:4326)')
        # check if the ID, latitude, longitude are provided
        if self.target_shp_ID == '':
            print('EASYMORE detects that no field for ID is provided in sink/target shapefile')
            print('arbitarary values of ID are added in the field ID_t')
            shp['ID_t']  = np.arange(len(shp))+1
        else:
            print('EASYMORE detects that the field for ID is provided in sink/target shapefile')
            # check if the provided IDs are unique
            ID_values = np.array(shp[self.target_shp_ID])
            if len(ID_values) != len(np.unique(ID_values)):
                sys.exit('The provided IDs in shapefile are not unique; provide unique IDs or do not identify target_shp_ID')
            shp['ID_t'] = shp[self.target_shp_ID]
        if self.target_shp_lat == '' or self.target_shp_lon == '':
            print('EASYMORE detects that either of the fields for latitude or longitude is not provided in sink/target shapefile')
            # in WGS84
            print('calculating centroid of shapes in WGS84 projection;')
            print('for better appximation use the easymore equal area centroid function to preprocess target shapefile')
            df_point = pd.DataFrame()
            warnings.simplefilter('ignore') # silent the warning
            df_point ['lat'] = shp.centroid.y
            df_point ['lon'] = shp.centroid.x
            warnings.simplefilter('default') # back to normal
        if self.target_shp_lat == '':
            print('EASYMORE detects that no field for latitude is provided in sink/target shapefile')
            print('latitude values are added in the field lat_t')
            shp['lat_t']  = df_point ['lat'] # centroid lat from target
        else:
            print('EASYMORE detects that the field latitude is provided in sink/target shapefile')
            shp['lat_t'] = shp[self.target_shp_lat]
        if self.target_shp_lon == '':
            print('EASYMORE detects that no field for longitude is provided in sink/target shapefile')
            print('longitude values are added in the field lon_t')
            shp['lon_t']  = df_point ['lon'] # centroid lon from target
        else:
            print('EASYMORE detects that the field longitude is provided in sink/target shapefile')
            shp['lon_t'] = shp[self.target_shp_lon]
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
            print('EASYMORE detects point(s) as geometry of target shapefile and will apply small buffer to them')
        if detected_multipoints:
            print('EASYMORE detected multipoint as geometry of target shapefile and will considere it as multipolygone')
            print('hence EASYMORE will provide the average of all the point in each multipoint')
            print('if you mistakenly have given poitns as multipoints please correct the target shapefile')
        if detected_lines:
            print('EASYMORE detected line as geometry of target shapefile and will considere it as polygon (adding small buffer)')
        print('it seems everything is OK with the sink/target shapefile; added to EASYMORE object target_shp_gpd')
        if self.sort_ID:
            shp = shp.sort_values(by='ID_t')
            shp = shp.reset_index(drop=True)
        shp['order'] = np.arange(len(shp)) + 1 # order of the shapefile
        return shp

    def check_source_nc (self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks the consistency of the dimentions and variables for source netcdf file(s)
        """
        flag_do_not_match = False
        nc_names = sorted(glob.glob (self.source_nc))
        if not nc_names:
            sys.exit('EASYMORE detects no netCDF file; check the path to the soure netCDF files')
        else:
            # when skip_check_all_source_nc is True easymore only
            # check the first file and not all the consistancy of all the files to the first file
            if self.skip_check_all_source_nc and len(nc_names)>1:
                nc_names = [nc_names[0]]
                print('EASYMORE only checks the first of source netcdf files for consistency of variables and dimensions '+\
                      'and assumes other netcdf source files are consistent with the first file: ', nc_names[0])
            # continue checking
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
                # get the variable information of lat, lon and dimensions of the variable.
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
            # check variable time and dimension time are the same name so time is coordinate
            for nc_name in nc_names:
                ncid = nc4.Dataset(nc_name)
                temp = ncid.variables[self.var_time].dimensions
                if len(temp) != 1:
                    sys.exit('EASYMORE expects 1D time variable, it seems time variables has more than 1 dimension')
                if str(temp[0]) != self.var_time:
                    sys.exit('EASYMORE expects time variable and dimension to be different, they should be the same\
                    for xarray to consider time dimension as coordinates')
        if flag_do_not_match:
            sys.exit('EASYMORE detects that all the provided netCDF files and variables \
has different dimensions for the variables or latitude and longitude')
        else:
            print('EASYMORE detects that the variables from the netCDF files are identical\
in dimensions of the variables and latitude and longitude')
            print('EASYMORE detects that all the variables have dimensions of:')
            print(var_dim)
            print('EASYMORE detects that the longitude variables has dimensions of:')
            print(lon_dim)
            print('EASYMORE detects that the latitude variables has dimensions of:')
            print(lat_dim)

    def check_source_nc_shp (self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks the source netcdf file shapefile
        needs more development
        """
        # load the needed packages
        import geopandas as gpd
        from   shapely.geometry import Polygon
        #import shapefile # pyshed library
        import shapely
        #
        multi_source = False
        nc_names = glob.glob (self.source_nc)
        ncid = nc4.Dataset(nc_names[0])
        # sink/target shapefile is what we want the variables to be remapped to
        shp = gpd.read_file(self.source_shp)
        if not self.check_shp_crs(shp):
            sys.exit('please project your source shapefile and variables in source nc files to WGS84 (epsg:4326)')
        else: # check if the projection is WGS84 (or epsg:4326)
            print('EASYMORE detects that source shapefile is in WGS84 (epsg:4326)')
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


    def check_shp_crs (self, shp, check_list = ['epsg:4326', 'epsg 4326', 'epsg: 4326', \
                                                'wgs84', 'wgs:84', 'wgs 84', 'wgs_84'\
                                                'wgs1984', 'wgs:1984', 'wgs 1984', 'wgs_1984']):

        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks the shapefile crs to provided substrings
        Arguments
        ---------
        shp: geopandas dataframe
        check_list: list of substring to check against
        """

        conforming = False

        # check if name is available in the check_list
        if str(shp.crs.name).lower() in check_list:
            conforming = True
        #else:
        #    epsg_code = shp.crs.to_epsg()
        #    print(f'EPSG:{epsg_code}')

        # check if the shp.crs include the element
        for element in check_list:
            if element in str(shp.crs).lower():
                conforming = True
        return conforming


    def NetCDF_SHP_lat_lon(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function checks dimension of the source shapefile and checks the case of regular, rotated, and irregular
        also created the 2D array of lat and lon for creating the shapefile
        """
        import geopandas as gpd
        from   shapely.geometry import Polygon
        #import shapefile # pyshed library
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
            print('EASYMORE detects case 1 - regular lat/lon')
            self.case = 1
            # get the list of dimensions for the ncid sample variable
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
            lat_temp_diff_2 = np.diff(lat_temp_diff)
            max_lat_temp_diff_2 = max(abs(lat_temp_diff_2))
            print('max difference of lat values in source nc files are : ', max_lat_temp_diff_2)
            lon_temp = np.array(ncid.variables[self.var_lon][:])
            lon_temp_diff = np.diff(lon_temp)
            lon_temp_diff_2 = np.diff(lon_temp_diff)
            max_lon_temp_diff_2 = max(abs(lon_temp_diff_2))
            print('max difference of lon values in source nc files are : ', max_lon_temp_diff_2)
            # save lat, lon into the object
            lat      = np.array(lat).astype(float)
            lon      = np.array(lon).astype(float)
            self.lat = lat
            self.lon = lon
            # expanding just for the the creation of shapefile with first last rows and columns
            if (max_lat_temp_diff_2<self.tolerance) and (max_lon_temp_diff_2<self.tolerance): # then lat lon are spaced equal
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
                # pass to the lat, lon extended
                self.lat_expanded = lat_expanded
                self.lon_expanded = lon_expanded
        # case #2 rotated lat/lon
        if (len(ncid.variables[self.var_lat].dimensions)==2) and (len(ncid.variables[self.var_lon].dimensions)==2):
            print('EASYMORE detects case 2 - rotated lat/lon')
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
            print('EASYMORE detects case 3 - irregular lat/lon; shapefile should be provided')
            self.case = 3
            lat = ncid.variables[self.var_lat][:]
            lon = ncid.variables[self.var_lon][:]
            #print(lat, lon)
            if self.var_ID  == '':
                print('EASYMORE detects that no variable for ID of the source netCDF file; an arbitatiry ID will be added')
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
                    crs = None,
                    file_name = None):
        """
        @ author:                  Shervan Gharari, Wouter Knoben
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a shapefile for the source netcdf file
        Arguments
        ---------
        lat: numpy array, the 2D matrix of lat_2D [n,m,]
        lon: numpy array, the 2D matrix of lon_2D [n,m,]
        file_name: string, name of the file that the shapefile will be saved at
        """

        from   shapely.geometry import Polygon
        import geopandas as gpd

        # empty dataframe
        df = pd.DataFrame()
        # get the lats and lons of surrounding grids
        df['Lat_Up_Left']   = lat [  :-2 ,   :-2].flatten()
        df['Lat_Left']      = lat [ 1:-1 ,   :-2].flatten()
        df['Lat_Low_Left']  = lat [ 2:   ,   :-2].flatten()
        df['Lat_Low']       = lat [ 2:   ,  1:-1].flatten()
        df['Lat_Low_Right'] = lat [ 2:   ,  2:  ].flatten()
        df['Lat_Right']     = lat [ 1:-1 ,  2:  ].flatten()
        df['Lat_Up_Right']  = lat [  :-2 ,  2:  ].flatten()
        df['Lat_Up']        = lat [  :-2 ,  1:-1].flatten()
        df['Lon_Up_Left']   = lon [  :-2 ,   :-2].flatten()
        df['Lon_Left']      = lon [ 1:-1 ,   :-2].flatten()
        df['Lon_Low_Left']  = lon [ 2:   ,   :-2].flatten()
        df['Lon_Low']       = lon [ 2:   ,  1:-1].flatten()
        df['Lon_Low_Right'] = lon [ 2:   ,  2:  ].flatten()
        df['Lon_Right']     = lon [ 1:-1 ,  2:  ].flatten()
        df['Lon_Up_Right']  = lon [  :-2 ,  2:  ].flatten()
        df['Lon_Up']        = lon [  :-2 ,  1:-1].flatten()
        # get the center of grid
        df['Lat_C']         = lat [ 1:-1 ,  1:-1].flatten()
        df['Lon_C']         = lon [ 1:-1 ,  1:-1].flatten()


        # calculate the mid point with surrounding grids
        df['Point_Lat_Up_Left']   = (df['Lat_Up_Left']   +df['Lat_Up']  +df['Lat_Left']   +df['Lat_C'])/4
        df['Point_Lat_Left']      = (df['Lat_Left']                                       +df['Lat_C'])/2
        df['Point_Lat_Low_Left']  = (df['Lat_Low_Left']  +df['Lat_Low'] +df['Lat_Left']   +df['Lat_C'])/4
        df['Point_Lat_Low']       = (df['Lat_Low']                                        +df['Lat_C'])/2
        df['Point_Lat_Low_Right'] = (df['Lat_Low_Right'] +df['Lat_Low'] +df['Lat_Right']  +df['Lat_C'])/4
        df['Point_Lat_Right']     = (df['Lat_Right']                                      +df['Lat_C'])/2
        df['Point_Lat_Up_Right']  = (df['Lat_Up_Right']  +df['Lat_Up']  +df['Lat_Right']  +df['Lat_C'])/4
        df['Point_Lat_Up']        = (df['Lat_Up']                                         +df['Lat_C'])/2
        df['Point_Lon_Up_Left']   = (df['Lon_Up_Left']   +df['Lon_Up']  +df['Lon_Left']   +df['Lon_C'])/4
        df['Point_Lon_Left']      = (df['Lon_Left']                                       +df['Lon_C'])/2
        df['Point_Lon_Low_Left']  = (df['Lon_Low_Left']  +df['Lon_Low'] +df['Lon_Left']   +df['Lon_C'])/4
        df['Point_Lon_Low']       = (df['Lon_Low']                                        +df['Lon_C'])/2
        df['Point_Lon_Low_Right'] = (df['Lon_Low_Right'] +df['Lon_Low'] +df['Lon_Right']  +df['Lon_C'])/4
        df['Point_Lon_Right']     = (df['Lon_Right']                                      +df['Lon_C'])/2
        df['Point_Lon_Up_Right']  = (df['Lon_Up_Right']  +df['Lon_Up']  +df['Lon_Right']  +df['Lon_C'])/4
        df['Point_Lon_Up']        = (df['Lon_Up']                                         +df['Lon_C'])/2

        # inner function to create the grid polygons
        def make_polygon(row):
            coords = [(row["Point_Lon_Up"]        , row["Point_Lat_Up"]), \
                      (row["Point_Lon_Up_Right"]  , row["Point_Lat_Up_Right"]), \
                      (row["Point_Lon_Right"]     , row["Point_Lat_Right"]), \
                      (row["Point_Lon_Low_Right"] , row["Point_Lat_Low_Right"]), \
                      (row["Point_Lon_Low"]       , row["Point_Lat_Low"]), \
                      (row["Point_Lon_Low_Left"]  , row["Point_Lat_Low_Left"]), \
                      (row["Point_Lon_Left"]      , row["Point_Lat_Left"]), \
                      (row["Point_Lon_Up_Left"]   , row["Point_Lat_Up_Left"]), \
                      (row["Point_Lon_Up"]        , row["Point_Lat_Up"])]
            return Polygon(coords)

        # Create a new column "geometry" in the DataFrame containing Polygons
        df["geometry"] = df.apply(make_polygon, axis=1)
        df = df.loc[:, ['geometry', 'Lat_C', 'Lon_C']]
        df ['ID_s'] = np.arange(len(df))+1
        df = df.rename(columns={'Lat_C': 'lat_s',
                                'Lon_C': 'lon_s'})
        # to geodataframe
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        # assining the crs
        if crs:
            gdf = gdf.set_crs(crs)
        # saving the file if file_name is provided
        if file_name:
            gdf.to_file(file_name)
        return gdf




    def add_lat_lon_source_SHP( self,
                                shp,
                                source_shp_lat,
                                source_shp_lon,
                                source_shp_ID):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
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
        #import shapefile # pyshed library
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
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
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
        #import shapefile # pyshed library
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
                print('EASYMORE decides the netCDF file has longtitude values of 0 to 360; creating the extended')
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
                print('EASYMORE decides the netCDF file has longtitude values of -180 to 180; creating the extended')
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
                sys.exit('EASYMORE cannot decide about the lat and lon of the shapefiles')
        result = gpd.read_file(temp_dir+case_name+'_source_shapefile_expanded.shp')
        return result

    def create_remap(   self,
                        int_df,
                        lat_source,
                        lon_source):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
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
        int_df: dataframe, including the associated rows and cols and EASYMORE case
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
        int_df['easymore_case'] = self.case
        # save remap_df as csv for future use
        return int_df

    def create_row_col_df ( self,
                            lat_source,
                            lon_source,
                            lat_target_int,
                            lon_target_int):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
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

    def check_easymore_remap(self,
                             remap_df):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
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
        # check if there is EASYMORE_case in the columns
        if 'easymore_case' in remap_df.columns:
            print('EASYMORE case exists in the remap file')
        else:
            sys.exit('EASYMORE case field does not exits in the remap file; make sure to include this if you create the remapping file manually!')
        # check if all the easymore_case is unique for the data set
        if not (len(np.unique(np.array(remap_df['easymore_case'])))==1):
            sys.exit('the EASYMORE_case is not unique in the remapping file')
        if not (np.unique(np.array(remap_df['easymore_case'])) == 1 or\
        np.unique(np.array(remap_df['easymore_case'])) == 2 or\
        np.unique(np.array(remap_df['easymore_case'])) == 3):
            sys.exit('EASYMORE case should be one of 1, 2 or 3; please refer to the documentation')
        self.case = np.unique(np.array(remap_df['easymore_case']))
        # check if the needed columns are existing
        if not set(['ID_t','lat_t','lon_t','order_t','ID_s','lat_s','lon_s','weight']) <= set(remap_df.columns):
            sys.exit('provided remapping file does not have one of the needed fields: \n'+\
                'ID_t, lat_t, lon_t, order_t, ID_s, lat_s, lon_s, weight')

    def __target_nc_creation(self):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This funciton read different grids and sum them up based on the
        weight provided to aggregate them over a larger area
        """
        print('------REMAPPING------')
        remap = pd.read_csv(self.remap_csv)
        remap = remap.apply(pd.to_numeric, errors='coerce') # convert non numeric to NaN
        # creating the target_ID_lat_lon
        target_ID_lat_lon = pd.DataFrame()
        target_ID_lat_lon ['ID_t']  = remap ['ID_t']
        target_ID_lat_lon ['lat_t'] = remap ['lat_t']
        target_ID_lat_lon ['lon_t'] = remap ['lon_t']
        target_ID_lat_lon ['order_t'] = remap ['order_t']
        target_ID_lat_lon = target_ID_lat_lon.drop_duplicates()
        target_ID_lat_lon = target_ID_lat_lon.sort_values(by=['order_t'])
        target_ID_lat_lon = target_ID_lat_lon.reset_index(drop=True)
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
        # check compression choice
        if isinstance(self.complevel, int) and (self.complevel >=1) and (self.complevel<=9):
            compflag = True
            complevel = self.complevel
            print('netcdf output file will be compressed at level', complevel)
        else:
            compflag = False
            complevel = 0
            print('netcdf output file will not be compressed.')
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
                sys.exit('units is not provided for the time variable for source NetCDF of'+ nc_name)
            if 'calendar' in ncids.variables[self.var_time].ncattrs():
                time_cal = ncids.variables[self.var_time].calendar
            else:
                sys.exit('calendar is not provided for the time variable for source NetCDF of'+ nc_name)
            time_var = ncids[self.var_time][:]
            self.length_of_time = len(time_var)
            target_date_times = nc4.num2date(time_var,units = time_unit,calendar = time_cal)
            target_name = self.output_dir + self.case_name + '_remapped_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.nc'
            if os.path.exists(target_name):
                if self.overwrite_remapped_nc:
                    print('Removing existing remapped .nc file.')
                    os.remove(target_name)
                else:
                    print('Remapped .nc file already exists. Going to next file.')
                    continue # skip to next file
            for var in ncids.variables.values():
                if var.name == self.var_time:
                    time_dtype =  str(var.dtype)
            time_dtype_code = 'f8' # initialize the time as float
            if 'float' in time_dtype.lower():
                time_dtype_code = 'f8'
            elif 'int' in time_dtype.lower():
                time_dtype_code = 'i4'
            # reporting
            statement_print = 'Remapping '+nc_name+' to '+target_name+' \n'
            time_start = datetime.now()
            statement_print = statement_print + 'Started at date and time '+ str(time_start) + ' \n'
            with nc4.Dataset(target_name, "w", format="NETCDF4") as ncid: # creating the NetCDF file
                # define the dimensions
                dimid_N = ncid.createDimension(self.remapped_dim_id, len(hruID_var))  # limited dimensiton equal the number of hruID
                dimid_T = ncid.createDimension('time', None)   # unlimited dimensiton
                # Variable time
                time_varid = ncid.createVariable('time', time_dtype_code, ('time', ), zlib=compflag, complevel=complevel)
                # Attributes
                time_varid.long_name = self.var_time
                time_varid.units = time_unit  # e.g. 'days since 2000-01-01 00:00' should change accordingly
                time_varid.calendar = time_cal
                time_varid.standard_name = self.var_time
                time_varid.axis = 'T'
                time_varid[:] = time_var
                # Variables lat, lon, subbasin_ID
                lat_varid = ncid.createVariable(self.remapped_var_lat, 'f8', (self.remapped_dim_id, ), zlib=compflag, complevel=complevel)
                lon_varid = ncid.createVariable(self.remapped_var_lon, 'f8', (self.remapped_dim_id, ), zlib=compflag, complevel=complevel)
                hruId_varid = ncid.createVariable(self.remapped_var_id, 'f8', (self.remapped_dim_id, ), zlib=compflag, complevel=complevel)
                # Attributes
                lat_varid.long_name = self.remapped_var_lat
                lon_varid.long_name = self.remapped_var_lon
                hruId_varid.long_name = 'shape ID'
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
                ncid.Source = 'Case: ' +self.case_name + '; remapped by script from library of Shervan Gharari (https://github.com/ShervanGharari/EASYMORE).'
                # write variables
                # check chunking choice
                if self.remapped_chunk_size == None:
                    chunk_sizes = None # Use netCDF4 default values, i.e. (1,n) chunk lengths for (unlimited,limited) dimensions, where n is the length of the limited dim
                else:
                    chunk_length = min(self.remapped_chunk_size, self.number_of_target_elements) # don't make a chunk > data length
                    chunk_sizes = (1,chunk_length) # (time,remap_dim)
                #loop over variables
                for i in np.arange(len(self.var_names)):
                    var_value  = self.__weighted_average( nc_name,
                                                          target_date_times,
                                                          self.var_names[i],
                                                          self.fill_value_list[i],
                                                          remap)
                    # Variables writing
                    varid = ncid.createVariable(self.var_names_remapped[i], \
                                                self.format_list[i], ('time',self.remapped_dim_id ),\
                                                fill_value = self.fill_value_list[i], zlib=compflag,\
                                                complevel=complevel,\
                                                chunksizes=chunk_sizes)
                    varid [:] = var_value
                    # Pass attributes
                    if 'long_name' in ncids.variables[self.var_names[i]].ncattrs():
                        varid.long_name = ncids.variables[self.var_names[i]].long_name
                    if 'units' in ncids.variables[self.var_names[i]].ncattrs():
                        varid.units = ncids.variables[self.var_names[i]].units
            if self.save_csv:
                ds = xr.open_dataset(target_name)
                for i in np.arange(len(self.var_names_remapped)):
                    # assign the variable to the data frame with the ID column name
                    column_name = list(map(str,list(np.array(ds[self.remapped_var_id]))))
                    column_name = ['ID_'+s for s in column_name]
                    df = pd.DataFrame(data=np.array(ds[self.var_names_remapped[i]]), columns=column_name)
                    # df ['time'] = ds.time
                    df.insert(loc=0, column='time', value=ds.time)
                    # get the unit for the variable if exists
                    unit_name = ''
                    if 'units' in ds[self.var_names_remapped[i]].attrs.keys():
                        unit_name = ds[self.var_names_remapped[i]].attrs['units']
                    # remove the forbidden character based on
                    # ['#','%','&','{','}','\','<','>','*','?','/',' ','$','!','`',''','"',':','@','+',',','|','=']
                    unit_name = re.sub("[#%&{}*<>*?*$!`:@+,|= ]","",unit_name)
                    unit_name = unit_name.replace("\\","")
                    unit_name = unit_name.replace("//","")
                    unit_name = unit_name.replace("/","")
                    # print(unit_name)
                    target_name_csv = self.output_dir + self.case_name + '_remapped_'+ self.var_names_remapped[i] +\
                     '_' + unit_name +\
                     '_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+ '.csv'
                    target_name_map = self.output_dir + 'Mapping_' + self.case_name + '_remapped_'+ self.var_names_remapped[i] +\
                     '_' + unit_name +\
                     '_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+ '.csv'
                    if os.path.exists(target_name_csv): # remove file if exists
                        os.remove(target_name_csv)
                    if os.path.exists(target_name_map): # remove file if exists
                        os.remove(target_name_map)
                    lat_data = np.squeeze(np.array(ds[self.remapped_var_lat])); lat_data = lat_data.flatten()
                    lon_data = np.squeeze(np.array(ds[self.remapped_var_lon])); lon_data = lon_data.flatten()
                    maps = np.zeros([2,len(lat_data)])
                    maps [0,:] = lat_data
                    maps [1,:] = lon_data
                    df_map = pd.DataFrame(data=maps, index=["lat","lon"], columns=column_name)
                    df.to_csv(target_name_csv)
                    df_map.to_csv(target_name_map)
                    statement_print = statement_print + 'Converting variable '+ self.var_names_remapped[i] +\
                    ' from remapped file of '+target_name+' to '+target_name_csv + ' \n'
                    statement_print = statement_print + 'Saving the ID, lat, lon map at '+target_name_csv+ ' \n'
            time_end = datetime.now()
            time_diff = time_end-time_start
            statement_print = statement_print + 'Ended at date and time ' + str(time_end) + ' \n'
            statement_print = statement_print + 'It took '+ str(time_diff.total_seconds()) +\
            ' seconds to finish the remapping of variable(s)' +' \n'+ ('---------------------')
            print(statement_print)
        print('---------------------')

    def __weighted_average(self,
                           nc_name,
                           target_time,
                           variable_name,
                           fill_value,
                           mapping_df):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function reads the data for a given time and calculates the weighted average
        Arguments
        ---------
        nc_name: string, name of the netCDF file
        target_time: string,
        variable_name: string, name of variable from source netcsf file to be remapped
        mapping_df: pandas dataframe, including the row and column of the source data and weight
        Returns
        -------
        weighted_value: a numpy array that has the remapped values from the nc file
        """
        # open dataset
        ds = xr.open_dataset(nc_name)
        # rename time variable to time
        if self.var_time != 'time':
            ds = ds.rename({self.var_time:'time'})
        # prepared the numpy array for ouptut
        weighted_value = np.zeros([self.length_of_time,self.number_of_target_elements])
        m = 0 # counter
        for date in target_time: # loop over time
            ds_temp = ds.sel(time=date.strftime("%Y-%m-%d %H:%M:%S"),method="nearest")
            data = np.array(ds_temp[variable_name])
            data = np.squeeze(data)
            # get values from the rows and cols and pass to np data array
            if self.case ==1 or self.case ==2:
                values = data [self.rows,self.cols]
            if self.case ==3:
                values = data [self.rows]
            values = np.array(values)
            # add values to data frame
            mapping_df['values'] = values
            # replace non-numeric or np.nan with NaN
            mapping_df['values'] = pd.to_numeric(mapping_df['values'], errors='coerce')
            # if there are NaN values then rescale the weights if needed
            there_is_nan = (mapping_df['values'].isna().any() or mapping_df['weight'].isna().any())
            if self.rescaledweights and there_is_nan:
                idx = mapping_df[~mapping_df['values'].isna() & ~mapping_df['weight'].isna()].index
                mapping_df['row'] = np.nan; mapping_df.loc[idx,'row'] = 1 # 1 for the once with values in both values and weight columns
                mapping_df['weight_row'] = mapping_df['weight'] * mapping_df['row']
                mapping_df['values_row'] = mapping_df['values'] * mapping_df['row']
                # rescaling the weight
                mapping_df['weight_row_sum']  = mapping_df['order_t'].map(mapping_df.groupby('order_t')['weight_row'].sum())
                mapping_df['weight_rescaled'] = mapping_df['weight'] / mapping_df['weight_row_sum'] # rescaled weight
                idx = mapping_df[mapping_df['weight_rescaled'] > 1.0].index
                mapping_df.loc[idx,'weight_rescaled'] = np.nan # if there is inf or devision by zero replace with nan
            else:
                mapping_df['weight_rescaled'] = mapping_df['weight']
            mapping_df['values_w']   = mapping_df['values'] * mapping_df['weight_rescaled']
            df_temp = mapping_df.groupby(['order_t'],as_index=False).agg({'values_w': 'sum'})
            if there_is_nan:
                idx                      = mapping_df[~mapping_df['values_w'].isna()].index
                mapping_df_slice         = mapping_df.loc[idx].copy()
                order_t_IDs_not_all_nan  = np.unique(mapping_df_slice['order_t'])
                order_t_IDs_all          = np.unique(mapping_df['order_t'])
                order_t_IDs_all_nan      = np.setdiff1d(order_t_IDs_all, order_t_IDs_not_all_nan).flatten()
                # put back NaN for the order_t that are all NaN values
                idx = df_temp[df_temp['order_t'].isin(order_t_IDs_all_nan)].index
                df_temp_slice = df_temp.loc[idx].copy()
                if not df_temp_slice.empty:
                    df_temp.loc[df_temp_slice.index,'values_w'] = np.nan
                df_temp['values_w'].fillna(fill_value, inplace=True)
            weighted_value [m,:] = np.array(df_temp['values_w'])
            m += 1
        return weighted_value


    def shp_lon_correction (self,
                            shp): # the name of SHP including path with WGS1984 projection
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function reads a shapefile and for longitude of model than 180 it correct it to a frame of -180 to 180
        ---------
        shp: geopandas shapefile
        Returns
        -------
        shp_final: geopandas shapefile corrected in the frame of -180 to 180
        """
        # loading the needed packaged
        import geopandas as gpd
        import pandas as pd
        from   shapely.geometry import Polygon
        import shapely
        # if no crs set to epsg:4326
        if not shp.crs:
            print('inside shp_lon_correction, no crs is provided for the shapefile; EASYMORE will allocate WGS84 \
to correct for lon above 180')
            shp = shp.set_crs("epsg:4326")
        #
        col_names = shp.columns.to_list()
        col_names.remove('geometry')
        df_attribute = pd.DataFrame()
        if col_names:
            df_attribute = shp.drop(columns = 'geometry')
        shp = shp.drop(columns = col_names)
        shp['ID'] = np.arange(len(shp))+1
        # get the maximum and minimum bound of the total bound
        min_lon, min_lat, max_lon, max_lat = shp.total_bounds
        min_lon = min_lon + self.tolerance
        max_lon = max_lon - self.tolerance
        shp_int1 = pd.DataFrame()
        shp_int2 = pd.DataFrame()
        if (360 < max_lon) and (min_lon<0):
            sys.exit('The minimum longitude is higher than 360 while the minimum longitude is lower that 0')
        if (max_lon < 180) and (-180 < min_lon):
            print('EASYMORE detects that shapefile longitude is between -180 and 180, no correction is performed')
            # shapefile with -180 to 180 lon
            gdf2 = {'geometry': [Polygon([( -180.0+self.tolerance, -90.0+self.tolerance), (-180.0+self.tolerance,  90.0-self.tolerance),\
                                          (  180.0-self.tolerance,  90.0-self.tolerance), ( 180.0-self.tolerance, -90.0+self.tolerance)])]}
            gdf2 = gpd.GeoDataFrame(gdf2)
            gdf2 = gdf2.set_crs ("epsg:4326")
            warnings.simplefilter('ignore')
            shp_int2 = self.intersection_shp(shp, gdf2)
            warnings.simplefilter('default')
            col_names = shp_int2.columns
            col_names = list(filter(lambda x: x.startswith('S_1_'), col_names))
            col_names.append('geometry')
            shp_int2 = shp_int2[shp_int2.columns.intersection(col_names)]
            col_names.remove('geometry')
            # rename columns without S_1_
            for col_name in col_names:
                col_name = str(col_name)
                col_name_n = col_name.replace("S_1_","");
                shp_int2 = shp_int2.rename(columns={col_name: col_name_n})
        else:
            # intersection if shp has a larger lon of 180 so it is 0 to 360,
            if (180 < max_lon) and (-180 < min_lon):
                print('EASYMORE detects that shapefile longitude is between 0 and 360, correction is performed to transfer to -180 to 180')
                # shapefile with 180 to 360 lon
                gdf1 = {'geometry': [Polygon([(  180.0+self.tolerance, -90.0+self.tolerance), ( 180.0+self.tolerance,  90.0-self.tolerance),\
                                              (  360.0-self.tolerance,  90.0-self.tolerance), ( 360.0-self.tolerance, -90.0+self.tolerance)])]}
                gdf1 = gpd.GeoDataFrame(gdf1)
                gdf1 = gdf1.set_crs ("epsg:4326")
                warnings.simplefilter('ignore')
                shp_int1 = self.intersection_shp(shp, gdf1)
                warnings.simplefilter('default')
                col_names = shp_int1.columns
                col_names = list(filter(lambda x: x.startswith('S_1_'), col_names))
                col_names.append('geometry')
                shp_int1 = shp_int1[shp_int1.columns.intersection(col_names)]
                col_names.remove('geometry')
                # rename columns without S_1_
                for col_name in col_names:
                    col_name = str(col_name)
                    col_name_n = col_name.replace("S_1_","");
                    shp_int1 = shp_int1.rename(columns={col_name: col_name_n})
                #
                for index, _ in shp_int1.iterrows():
                    polys = shp_int1.geometry.iloc[index] # get the shape
                    polys = shapely.affinity.translate(polys, xoff=-360.0, yoff=0.0, zoff=0.0)
                    shp_int1.geometry.iloc[index] = polys
                # shapefile with -180 to 180 lon
                gdf2 = {'geometry': [Polygon([( -180.0+self.tolerance, -90.0+self.tolerance), (-180.0+self.tolerance,  90.0-self.tolerance),\
                                              (  180.0-self.tolerance,  90.0-self.tolerance), ( 180.0-self.tolerance, -90.0+self.tolerance)])]}
                gdf2 = gpd.GeoDataFrame(gdf2)
                gdf2 = gdf2.set_crs ("epsg:4326")
                warnings.simplefilter('ignore')
                shp_int2 = self.intersection_shp(shp, gdf2)
                warnings.simplefilter('default')
                col_names = shp_int2.columns
                col_names = list(filter(lambda x: x.startswith('S_1_'), col_names))
                col_names.append('geometry')
                shp_int2 = shp_int2[shp_int2.columns.intersection(col_names)]
                col_names.remove('geometry')
                # rename columns without S_1_
                for col_name in col_names:
                    col_name = str(col_name)
                    col_name_n = col_name.replace("S_1_","");
                    shp_int2 = shp_int2.rename(columns={col_name: col_name_n})
            #
            if (max_lon < 180) and (min_lon < -180):
                print('EASYMORE detects that shapefile longitude is between 0 and 360, correction is performed to transfer to -180 to 180')
                # shapefile with -180 to -360 lon
                gdf1 = {'geometry': [Polygon([( -360.0+self.tolerance, -90.0+self.tolerance), (-360.0+self.tolerance,  90.0-self.tolerance),\
                                              ( -180.0-self.tolerance,  90.0-self.tolerance), (-180.0-self.tolerance, -90.0+self.tolerance)])]}
                gdf1 = gpd.GeoDataFrame(gdf1)
                gdf1 = gdf1.set_crs ("epsg:4326")
                warnings.simplefilter('ignore')
                shp_int1 = self.intersection_shp(shp, gdf1)
                warnings.simplefilter('default')
                col_names = shp_int1.columns
                col_names = list(filter(lambda x: x.startswith('S_1_'), col_names))
                col_names.append('geometry')
                shp_int1 = shp_int1[shp_int1.columns.intersection(col_names)]
                col_names.remove('geometry')
                # rename columns without S_1_
                for col_name in col_names:
                    col_name = str(col_name)
                    col_name_n = col_name.replace("S_1_","");
                    shp_int1 = shp_int1.rename(columns={col_name: col_name_n})
                #
                for index, _ in shp_int1.iterrows():
                    polys = shp_int1.geometry.iloc[index] # get the shape
                    polys = shapely.affinity.translate(polys, xoff=+360.0, yoff=0.0, zoff=0.0)
                    shp_int1.geometry.iloc[index] = polys
                # shapefile with -180 to 180 lon
                gdf2 = {'geometry': [Polygon([( -180.0+self.tolerance, -90.0+self.tolerance), (-180.0+self.tolerance,  90.0-self.tolerance),\
                                              (  180.0-self.tolerance,  90.0-self.tolerance), ( 180.0-self.tolerance, -90.0+self.tolerance)])]}
                gdf2 = gpd.GeoDataFrame(gdf2)
                gdf2 = gdf2.set_crs ("epsg:4326")
                warnings.simplefilter('ignore')
                shp_int2 = self.intersection_shp(shp, gdf2)
                warnings.simplefilter('default')
                col_names = shp_int2.columns
                col_names = list(filter(lambda x: x.startswith('S_1_'), col_names))
                col_names.append('geometry')
                shp_int2 = shp_int2[shp_int2.columns.intersection(col_names)]
                col_names.remove('geometry')
                # rename columns without S_1_
                for col_name in col_names:
                    col_name = str(col_name)
                    col_name_n = col_name.replace("S_1_","");
                    shp_int2 = shp_int2.rename(columns={col_name: col_name_n})
        # merging the two shapefiles
        if not shp_int1.empty and not shp_int2.empty:
            shp_final = pd.concat([shp_int1,shp_int2])
        elif not shp_int1.empty:
            shp_final = shp_int1
        elif not shp_int2.empty:
            shp_final = shp_int2
        # put back the pandas into geopandas
        shp_final = shp_final.set_geometry('geometry')
        shp_final = shp_final.dissolve(by='ID', as_index=False)
        shp_final = shp_final.sort_values(by='ID')
        shp_final = shp_final.drop(columns='ID')
        if not df_attribute.empty: # add attributes
            shp_final = pd.concat([shp_final, df_attribute], axis=1)
        # check if the output has the same number of elements
        if len(shp) != len(shp_final):
            sys.exit('the element of input shapefile and corrected shapefile area not the same')
        # return the shapefile
        return shp_final


    def shp_centroid_equal_area (self,
                                 shp):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function calcultes the centroid in equal area projection for shapefile bound between -180 to 180.
        ---------
        shp: geopandas shapefile
        Returns
        -------
        shp: geopandas shapefile with centroid in equal arae
        shp_points: geopandas shapefile (points) with location in WGS84 corresponsing to the centroid in equal area
        """
        # in equal projection
        print('calculating centroid of shapes in equal area projection')
        if not self.check_shp_crs(shp): # "epsg:4326" not in str(shp.crs).lower():
            sys.exit('shapefile should be in WGS84 projection');
        minx, miny, maxx, maxy = shp.total_bounds
        if maxx > 180:
            sys.exit('it seems that the shapefile has longitude values of more than 180 degree which might make \
            problem in equal area projection; other software can be used');
        shp_temp = shp.to_crs ("EPSG:6933") # source shapefile to equal area
        lat_c = np.array(shp_temp.centroid.y) # centroid lat from target
        lon_c = np.array(shp_temp.centroid.x) # centroid lon from target
        df_point = pd.DataFrame()
        df_point ['lat'] = lat_c
        df_point ['lon'] = lon_c
        shp_points = self.make_shape_point(df_point, 'lon', 'lat', crs="EPSG:6933")
        shp_points = shp_points.to_crs("EPSG:4326") # to WGS
        shp_points = shp_points.drop(columns=['lat','lon'])
        shp_points ['lat'] = shp_points.geometry.y
        shp_points ['lon'] = shp_points.geometry.x
        shp ['lat_cent'] = shp_points ['lat']
        shp ['lon_cent'] = shp_points ['lon']
        return shp, shp_points


    def dataframe_to_netcdf_xr (self,
                                data_frame,
                                nc_file_name = None,
                                nc_file_path = None,
                                data_frame_DateTime_column = None,
                                variable_name = 'variable',
                                variable_dim_name = 'n',
                                unit_of_variable = None,
                                variable_long_name = None,
                                Fill_value = None,
                                station_info_data = None,
                                station_info_column = None):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function get a pandas dataframe of some values (all float + datetime string) and save them in a netcdf file
        ---------
        data_frame: pandas data_frame with index as pandas data time or path to csv file
        nc_file_name: the name of nc file to be saved, string
        nc_file_path: the path to where the nc file will be saved, string
        data_frame_DateTime_column: in case data_frame is path to csv file then column data_frame_DateTime_column should be provided, string
        variable_name: name of variable name to be saved in nc file, string
        unit_of_variable: unit of variable name to be saved in nc file, string
        variable_long_name: variable long name to be saved in nc file, string
        Fill_value: fill value to be saved in nc file, string
        station_info_data: pandas dataframe or path to csv file including the information of stations or grid for data_frame
        station_info_column: the column that include the information of station from station_info_data, string
        """

        # preparation of dataset
        if isinstance(data_frame, pd.DataFrame):
            print('EASYMORE detects that the input datafarame is pandas dataframe')
            if isinstance(data_frame.index, pd.DatetimeIndex):
                print('EASYMORE detects that index is pandas datatime')
            else:
                sys.exit('EASYMORE detects that the index is not datetime in data input file. ',\
                         'You use dataframe.index = pd.to_datetime(dataframe.index) to make sure ',\
                         'the index is in <class pandas.core.indexes.datetimes.DatetimeIndex>')
        elif isinstance(data_frame, str):
            if data_frame_DateTime_column is None:
                sys.exit('dataframe is provided as string csv file,'\
                         +'please provide the name of time index column')
            # read the data csv
            data_frame = pd.read_csv(data_frame)
            # convert the data time of the data_frame to index
            data_frame[data_frame_DateTime_column] = pd.to_datetime(data_frame[data_frame_DateTime_column], infer_datetime_format=True)
            data_frame = data_frame.set_index(data_frame_DateTime_column) # set as index
            data_frame.index = pd.to_datetime(data_frame.index) # from index to datetime index
        else:
            sys.exit('The data input type is not recognized')
        data_frame = data_frame.rename_axis(index=None) # remove possible name of the index column

        # preparation of dataset_info
        # encoding for station info
        encoding = {}
        if not (station_info_data is None):
            if not station_info_column:
                sys.exit('The station name column should be provided')
            if isinstance(station_info_data, pd.DataFrame):
                print('EASYMORE detects that the station data is pandas dataframe')
            elif isinstance(station_info_data, str):
                if data_frame_DateTime_column is None:
                    sys.exit('dataframe is provided as string csv file, please'+\
                             ' provide the name of time index column')
                # read the data csv
                station_info_data = pd.read_csv(station_info_data)
            else:
                sys.exit('The station info data input type is not recognized')
            station_info_data = station_info_data.set_index(station_info_column)
            station_info_data = station_info_data.rename_axis(index=None)
            # check if all station info info is exsiting in the station info
            if set(list(data_frame.columns)) <= set(list(station_info_data.columns)):
                print('EASYMORE detects that the necessary information for the station are provided')
            else:
                # try tranposing the station info
                station_info_data = station_info_data.transpose()
                if set(list(data_frame.columns)) <= set(list(station_info_data.columns)):
                    print('EASYMORE detects that the necessary information for the station are provided with transpose')
                else:
                    sys.exit('EASYMORE detects the data frame provided for data, columns names, '+\
                             'are partly missing in the station information')
            # subset the station infromation for the provided data
            station_info_data = station_info_data [station_info_data.columns.intersection(list(data_frame.columns))]
            data_frame = data_frame[list(station_info_data.columns)] # reorder columns to match station into
            # encoding
            station_info_data_type = self.check_if_row_is_numeric (station_info_data)
            for index, row in station_info_data_type.iterrows():
                temp = {}
                if row['row_type'] == 'int':
                    temp = {index: {'dtype': 'int32'}}
                    station_info_data.loc[index] = station_info_data.loc[index].astype(int)
                if row['row_type'] == 'float':
                    temp = {index: {'dtype': 'float64'}}
                    station_info_data.loc[index] = station_info_data.loc[index].astype(float)
                encoding.update(temp)
            station_info_data = station_info_data.transpose()
        else:
            station_info_data = pd.DataFrame(np.arange(len(data_frame.columns))+1,\
                                             index=data_frame.columns,\
                                             columns=['ID'])
        # rename the dimension
        if station_info_data.index.name is None:
            station_info_data.index.name = variable_dim_name
        #
        info = xr.Dataset(station_info_data.to_xarray())

        # data
        data = xr.Dataset(data_vars=dict(values=(["time", station_info_data.index.name],\
                                                 np.array(data_frame))),\
                          coords=dict(time = data_frame.index))
        attr = {}
        if unit_of_variable:
            attr.update({'units' : unit_of_variable})
        if variable_long_name:
            attr.update({'long_name' : variable_long_name})
        data['values'].attrs=attr
        if variable_name:
            data = data.rename_vars({'values':variable_name})

        # merging the data and data information
        data = xr.merge([data, info])

        # save the file if nc_file is provided
        if not (nc_file_name is None):
            # there is nc_file_name to save
            if nc_file_path is None:
                nc_file_path = ''
            else:
                if (not os.path.isdir(nc_file_path)):
                    os.makedirs(nc_file_path)
            if Fill_value is None:
                Fill_value = -9999
            if data_frame.applymap(lambda x: isinstance(x, (int, float))).all().all():
                temp = {variable_name:{'dtype': 'object', '_FillValue': Fill_value}}
            else:
                temp = {variable_name:{'dtype': 'object'}}
            if os.path.isfile(nc_file_path+nc_file_name):
                os.remove(nc_file_path+nc_file_name)
            data.to_netcdf(nc_file_path+nc_file_name, encoding=temp)
            print("EASYMORE saved the nc file here: ",nc_file_path+nc_file_name)
        # return xarray dataset
        return data


    def check_if_row_is_numeric (self,
                                 df):

        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function get a pandas dataframe and determin if a column is all numeric (int or float) or others
        ---------
        df: pandas dataframe
        return:
        df_type: pandas data_frame with type of each row
        """
        df_type = df.copy()
        df_type ['row_type'] = 'NaN'
        for index, row in df.iterrows():
            row_is_numeric = True
            for value in row:
                try:
                    pd.to_numeric(value, errors='raise')
                except ValueError:
                    row_is_numeric = False
                    break
            if row_is_numeric:
                if self.check_if_row_is_float_or_int(row):
                    T = 'float'
                else:
                    T = 'int'
                df_type ['row_type'].loc[index] = T
        df_type = df_type.loc[:, ['row_type']].copy()
        return df_type


    def check_if_row_is_float_or_int (self,
                                      row):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function get a pandas dataframe row and check of the entire row is float or int
        ---------
        row: pandas data_frame row
        return
        has_float: boolean or logical, flag that indicate the row has a float
        """
        has_float = False
        for item in row:
            if isinstance(pd.to_numeric(item, errors='raise'), float):
                has_float = True
                break
        return has_float


    ##############################################################
    #### GIS section
    ##############################################################

    def intersection_shp(   self,
                            shp_1,
                            shp_2):
        import geopandas as gpd
        from   shapely.geometry import Polygon
        #import shapefile # pyshed library
        import shapely
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @license:                  GNU-GPLv3
        This function intersects two shapefiles. It keeps the attributes from the first and second shapefiles (identified by prefix S_1_ and
        S_2_). It also creates other fields including AS1 (area of the shape element from shapefile 1), IDS1 (an arbitrary index
        for the shapefile 1), AS2 (area of the shape element from shapefile 1), IDS2 (an arbitrary index for the shapefile 1),
        AINT (the area of the intersected shapes), AP1 (the area of the intersected shape to the shapes from shapefile 1),
        AP2 (the area of the intersected shape to the shapes from shapefile 2), AP1N (the area normalized in the case AP1
        summation is less than one for a given shape from shapefile 1, this will help to preserve mass if parts of the shapefile are not
        intersected, for example, shapefile overextends the existing domain of the other shapefile), AP2N (the area normalized in the case AP2
        summation is less than one for a given shape from shapefile 2, this will help to preserve mass if parts of the shapefile are not
        intersected, for example, shapefile overextends the existing domain of the other shapefile)
        Arguments
        ---------
        shp_1: geo data frame, shapefile 1
        shp_2: geo data frame, shapefile 2
        Returns
        -------
        result: a geodataframe that includes the intersected shapefile and area, percent and normalized percent of each shape
        elements in another one
        """
        # get the column name of shp_1
        column_names = shp_1.columns
        column_names = list(column_names)
        # removing the geometry from the column names
        column_names.remove('geometry')
        # renaming the column with S_1
        column_names_new = ['S_1_' + s for s in column_names]
        renaming = dict(zip(column_names, column_names_new))
        shp_1.rename(columns = renaming, inplace=True)
        # Caclulate the area for shp1
        shp_1['AS1']  = shp_1.area
        shp_1['IDS1'] = np.arange(shp_1.shape[0])+1
        # get the column name of shp_2
        column_names = shp_2.columns
        column_names = list(column_names)
        # removing the geometry from the colomn names
        column_names.remove('geometry')
        # renaming the column with S_2
        column_names_new = ['S_2_' + s for s in column_names]
        renaming = dict(zip(column_names, column_names_new))
        shp_2.rename(columns = renaming, inplace=True)
        # Caclulate the area for shp2
        shp_2['AS2']  = shp_2.area
        shp_2['IDS2'] = np.arange(shp_2.shape[0])+1
        # Intersection
        result = self.spatial_overlays (shp_1, shp_2, how='intersection')
        # Caclulate the area for shp2
        result['AINT'] = result['geometry'].area
        result['AP1']  = result['AINT']/result['AS1']
        result['AP2']  = result['AINT']/result['AS2']
        # Calculate the normalized area for AP1 and AP2 to conserve mass
        result['IDS1'] = result['IDS1'].astype(int)
        result['IDS2'] = result['IDS2'].astype(int)
        #
        df = pd.DataFrame()
        df['AP1'] = result['AP1'].values
        df['IDS1'] = result['IDS1'].values
        df['AP1N'] = df['AP1'] / df.groupby('IDS1')['AP1'].transform('sum')
        # df['AP1N'] = df.groupby('IDS1')['AP1'].apply(lambda x: (x / x.sum())).reset_index(drop=True)
        result['AP1N'] = df['AP1N'].values
        #
        df = pd.DataFrame()
        df['AP2'] = result['AP2'].values
        df['IDS2'] = result['IDS2'].values
        df['AP2N'] = df['AP2'] / df.groupby('IDS2')['AP2'].transform('sum')
        #df['AP2N'] = df.groupby('IDS2')['AP2'].apply(lambda x: (x / x.sum())).reset_index(drop=True)
        result['AP2N'] = df['AP2N'].values
        # return
        return result

    def spatial_overlays(self,
                         df1,
                         df2,
                         how='intersection',
                         reproject=True):
        import geopandas as gpd
        from   shapely.geometry import Polygon
        #import shapefile # pyshed library
        import shapely
        """
        Perform spatial overlay between two polygons.
        Currently only supports data GeoDataFrames with polygons.
        Implements several methods that are all effectively subsets of
        the union.
        author: Omer Ozak (with his permission)
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
        reprojet: boolean, to reproject one shapefile to another crs
                  for spatial operation
        Returns
        -------
        df: GeoDataFrame
            GeoDataFrame with a new set of polygons and attributes
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
            #pairs = gpd.GeoDataFrame(nei, columns=['idx1','idx2'], crs=df1.crs)
            #pairs = gpd.GeoDataFrame(nei, columns=['idx1','idx2'])
            pairs = pd.DataFrame(nei, columns=['idx1','idx2']) # replace instead of initializing the GeoDataFrame
            pairs = pairs.merge(df1, left_on='idx1', right_index=True)
            pairs = pairs.merge(df2, left_on='idx2', right_index=True, suffixes=['_1','_2'])
            pairs['Intersection'] = pairs.apply(lambda x: (x['geometry_1'].intersection(x['geometry_2'])).buffer(0), axis=1)
            #pairs = gpd.GeoDataFrame(pairs, columns=pairs.columns, crs=df1.crs)
            pairs = gpd.GeoDataFrame(pairs, columns=pairs.columns)
            cols = pairs.columns.tolist()
            cols.remove('geometry_1')
            cols.remove('geometry_2')
            cols.remove('sidx')
            cols.remove('bbox')
            cols.remove('Intersection')
            dfinter = pairs[cols+['Intersection']].copy()
            dfinter.rename(columns={'Intersection':'geometry'}, inplace=True)
            #dfinter = gpd.GeoDataFrame(dfinter, columns=dfinter.columns, crs=pairs.crs)
            dfinter = gpd.GeoDataFrame(dfinter, columns=dfinter.columns, crs=df1.crs)
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

    def make_shape_point(   self,
                            dataframe,
                            lon_column,
                            lat_column,
                            point_shp_file_name = None,
                            crs = None):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a geopandas dataframe of lat, lon, and IDs provided from a csv file or a pandas dataframe
        Arguments
        ---------
        dataframe: pandas dataframe or string linking to a csv data frame that includes the lat/lon and other info
        lon_column: string, the name of the longitude column in dataframe
        lat_column: string, the name of the latitude column in dataframe
        point_shp_file_name: string, the name of the point shapefile to be saved
        crs: string, indicating the spatial reference; e.g.'EPSG:4326'
        Returns
        -------
        shp: geopandas dataframe, with the geometry of longitude and latitude
        """
        import geopandas as gpd
        import pandas    as pd
        # if string is given it is assumed that is the link to a csv file including lat and lon of the points
        if isinstance(dataframe, str):
            dataframe = pd.read_csv(dataframe)
        # create the GeoDataFrame
        shp = gpd.GeoDataFrame(dataframe, geometry=gpd.points_from_xy(dataframe[lon_column], dataframe[lat_column]))
        # assign crs, otherwise assign the default
        if crs:
            shp = shp.set_crs (crs)
            print('crs ', crs,' is assigned to the point shapefile')
        if point_shp_file_name:
            shp.to_file(point_shp_file_name)
            print('point shapefile is saved at ', point_shp_file_name)
        return shp

    def shp_from_irregular_nc (self,
                               station_shp_file_name = None,
                               voronoi_shp_file_name = None,
                               buffer = 2,
                               crs = None,
                               tolerance = 0.00001):
        """
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function creates a Voronoi diagram from latitude and longitude provided in an irregular shapefile
        Arguments
        ---------
        station_shp_file_name: string, name of the created point shapefile to be saved
        voronoi_shp_file_name: string, name of the created Voronoi shapefile to be saved
        buffer: float, the buffer around the bounding box of the point shapefile for creating the Voronoi shapefile
        crs: string, indicating the spatial reference system; e.g.'EPSG:4326'
        tolerance: float, tolerance to identify if two points provided in nc files are closer than tolerance
        Returns
        -------
        voronoi: geopandas dataframe, Voronio diagram
        points: geopandas dataframe, points shapefile created for irregular nc file (e.g. station data)
        """

        # read the file (or the first file)
        nc_names = glob.glob(self.source_nc)
        ncid     = nc4.Dataset(nc_names[0])
        # create the data frame
        points = pd.DataFrame()
        points['lon'] = ncid.variables[self.var_lon][:]
        points['lat'] = ncid.variables[self.var_lat][:]
        points['ID_s'] = 1 + np.arange(len(points))
        if self.var_ID != '':
            points['ID_s'] = ncid.variables[self.var_ID][:]
        else:
            points['ID_s'] = 1 + np.arange(len(points))
        points['ID_test'] = points['ID_s']
        if self.var_station != '':
            points['station_name'] = ncid.variables[self.var_station][:]
        # check if two points fall on each other
        points = points.sort_values(by=['lat','lon'])
        points['lon_next'] = np.roll(points['lon'],1)
        points['lat_next'] = np.roll(points['lat'],1)
        points['ID_test_next'] = np.roll(points['ID_test'], 1)
        points['distance'] = np.power(points['lon_next']-points['lon'], 2)+\
                             np.power(points['lat_next']-points['lat'], 2)
        points['distance'] = np.power(points['distance'], 0.5)
        idx = points.index[points['distance']<tolerance]
        if not (idx.empty):
            # add small values to overcome the issue of overlapping points
            print('EASYMORE detects that the lat lon values are for 2 or'+\
                  'more points are identical given the tolerance values of ',str(tolerance))
            print('ID of those points are:')
            print(points['ID_s'].loc[idx].values)
            #points['lat'].loc[idx]=points['lat'].loc[idx]+tolerance
            #points['lon'].loc[idx]=points['lon'].loc[idx]+tolerance
            points.loc[idx,'lat']=points.loc[idx,'lat']+tolerance
            points.loc[idx,'lon']=points.loc[idx,'lon']+tolerance
        points = points.sort_values(by='ID_s')
        points.rename(columns = {'lat':'lat_s','lon':'lon_s'},inplace=True)
        points = points.drop(columns=['lon_next','lat_next','ID_test','ID_test_next','distance'])
        # making points
        points = self.make_shape_point(points,
                                       'lon_s',
                                       'lat_s',
                                        point_shp_file_name = station_shp_file_name,
                                        crs = crs)
        # creating the voronoi diagram
        voronoi = self.voronoi_diagram(points,
                                       ID_field_name = 'ID_s',
                                       voronoi_shp_file_name = voronoi_shp_file_name)
        # return the shapefile
        return voronoi, points

    def voronoi_diagram(self,
                        points_shp_in,
                        ID_field_name=None,
                        voronoi_shp_file_name=None,
                        buffer = 2):
        """
        original code by:
        Abdishakur
        https://towardsdatascience.com/how-to-create-voronoi-regions-with-geospatial-data-in-python-adbb6c5f2134
        modified by:
        @ author:                  Shervan Gharari
        @ Github:                  https://github.com/ShervanGharari/EASYMORE
        @ author's email id:       sh.gharari@gmail.com
        @ license:                 GNU-GPLv3
        This function reads a shapefile of points and returns the Thiessen or Voronoi polygons for that point shapefile
        ---------
        points_shp_in: geopandas or string; if string it will read the shapefile
        ID_field_name: string, the name of attributes that include ID index values (optional)
        voronoi_shp_file_name: string, name of the Voronoi output shapefile to be saved
        buffer: float, the buffer around the points bounding box to limit the Voronoi diagram
        Returns
        -------
        Thiessen: geopandas dataframe, Voronio or Thiessen diagram
        """
        #import shapefile # as part of pyshp
        import geovoronoi
        import os
        from   shapely.geometry import Polygon
        from   shapely.geometry import box
        import numpy as np
        import pandas as pd
        import geopandas as gpd
        # read the shapefile
        if isinstance(points_shp_in, str):
            stations = gpd.read_file(points_shp_in)
        else:
            stations = points_shp_in # geodataframe
        # get the crs from the point shapefile
        crs_org = stations.crs
        print('crs from the point geopandas: ', crs_org)
        if (crs_org == 'None') or (crs_org is None):
            print('crs from the point geopandas is not defined and will be assigned as WGS84')
            stations = stations.set_crs('epsg:4326')
            crs_org = stations.crs
        # add the ID_t to the point shapefiles
        if not ID_field_name:
            stations ['ID_s'] = np.arange(len(stations))+1
            stations ['ID_s'] = stations ['ID_s'].astype(int)
        else:
            stations ['ID_s'] = stations[ID_field_name].astype(int)
        stations = stations.sort_values(by='ID_s')
        ID_s = stations ['ID_s']
        # get the total boundary of the shapefile
        stations_buffert = stations.buffer(buffer) # add a buffer
        minx, miny, maxx, maxy = stations_buffert.total_bounds
        bbox = box(minx, miny, maxx, maxy)
        gdf = gpd.GeoDataFrame(geometry=[bbox])
        gdf.to_file(self.temp_dir+'test.shp')
        # # create the bounding shapefile
        # parts = []
        # with shapefile.Writer(self.temp_dir+'test.shp') as w:
        #     w.autoBalance = 1 # turn on function that keeps file stable if number of shapes and records don't line up
        #     w.field("ID_bounding",'N') # create (N)umerical attribute fields, integer
        #     # creating the polygon given the lat and lon
        #     parts.append([ (minx, miny),\
        #                    (minx, maxy),\
        #                    (maxx, maxy),\
        #                    (maxx, miny),\
        #                    (minx, miny)])
        #     # store polygon
        #     w.poly(parts)
        #     # update records/fields for the polygon
        #     w.record(1)

        boundary = gpd.read_file(self.temp_dir+'test.shp')
        for f in glob.glob(self.temp_dir+'test.*'):
            os.remove(f)
        # create the voroni diagram for given point shapefile
        coords = geovoronoi.points_to_coords(stations.geometry)
        poly_shapes, location = \
        geovoronoi.voronoi_regions_from_coords(coords, boundary.iloc[0].geometry)
        # pass te polygons to shapefile
        Thiessen = gpd.GeoDataFrame()
        for i in np.arange(len(poly_shapes)):
            Thiessen.loc[i, 'geometry'] = Polygon(poly_shapes[i])
            Thiessen.loc[i, 'ID_s']     = stations.iloc[location[i][0]].ID_s
        Thiessen['ID_s'] = Thiessen['ID_s'].astype(int)
        Thiessen = Thiessen.sort_values(by='ID_s')# sort on values
        stations = stations.drop(columns='geometry')
        Thiessen = pd.merge_asof(Thiessen, stations, on='ID_s') #, direction='nearest')
        Thiessen = Thiessen.set_geometry('geometry') #bring back the geometry filed; pd to gpd
        Thiessen = Thiessen.set_crs(crs_org)
        ID_s_V = Thiessen['ID_s']
        diff = np.setdiff1d(ID_s, ID_s_V, assume_unique=False)
        if diff.size !=0 :
            print(diff)
            sys.exit('It seems the input points with the following ID do have identical longitude and latitude')
        if not (voronoi_shp_file_name is None):
            Thiessen.to_file(voronoi_shp_file_name)
        return Thiessen


    ##############################################################
    #### visualization section
    ##############################################################

    def nc_vis(self,
               source_nc_name                  = None,
               source_nc_var_lon               = None,
               source_nc_var_lat               = None,
               source_nc_var_ID                = None,
               source_nc_var_time              = None,
               source_nc_var_name              = None,
               source_shp_name                 = None,
               source_shp_field_ID             = None,
               source_shp_field_lat            = None,
               source_shp_field_lon            = None,
               source_shp_center_flag          = False,
               source_shp_center_color         = 'red',
               remapped_nc_name                = None,
               remapped_nc_var_ID              = None,
               remapped_nc_var_time            = None,
               remapped_nc_var_name            = None,
               target_shp_name                 = None,
               target_shp_field_ID             = None,
               target_shp_field_lat            = None,
               target_shp_field_lon            = None,
               target_shp_center_flag          = False,
               target_shp_center_color         = 'red',
               time_step_of_viz                = None,
               location_save_fig               = None,
               fig_name                        = None,
               fig_size                        = None,
               show_target_shp_flag            = None,
               show_remapped_values_flag       = None,
               show_source_flag                = True,
               cmap                            = None,
               margin                          = 0.1, #degree
               linewidth_source                = 1,
               linewidth_remapped              = 1,
               alpha_source                    = 1,
               alpha_remapped                  = 1,
               font_size                       = 40,
               font_family                     = 'Times New Roman',
               font_weigth                     = 'bold',
               add_colorbar_flag               = True,
               min_value_colorbar              = None,
               max_value_colorbar              = None,
               min_lon                         = None,
               min_lat                         = None,
               max_lon                         = None,
               max_lat                         = None):

        import xarray              as      xr
        from   matplotlib          import  pyplot as plt
        import matplotlib          as      mpl
        import geopandas           as      gpd
        import pandas              as      pd
        import os
        import numpy               as      np
        from   datetime            import  datetime
        import sys
        #
        font = {'family' :  font_family,
                'weight' :  font_weigth,
                'size'   :  font_size}
        mpl.rc('font', **font)
        #
        colorbar_do_not_exists = True
        # initializing EASYMORE object and find the case of source netcdf file
        # check of the source_nc_names is string and doesnt have * in it
        if isinstance(source_nc_name, str):
            if ('*' in source_nc_name):
                sys.exit('you should provide one file name as string and do not use * ')
        else:
            sys.exit('source nc name should be string and without *, should be the link to one file')
        self.source_nc = source_nc_name # pass that to the easymore object
        # check of the source_nc_names
        if isinstance(source_nc_var_name, str):
            self.var_names            = [source_nc_var_name] # string to list
        else:
            sys.exit('the variable should be only one and in string format')
        self.var_lon                  = source_nc_var_lon
        self.var_lat                  = source_nc_var_lat
        #
        remapped_nc_exists = False
        if remapped_nc_name:
            remapped_nc_exists = True
        # deciding the case
        self.NetCDF_SHP_lat_lon() # to find the case and lat/lon
        #
        ds_source = xr.open_dataset(source_nc_name) # source
        if source_shp_name:
            shp_source = gpd.read_file(source_shp_name)
        # get the step for the remapped
        date = pd.DatetimeIndex(ds_source[source_nc_var_time].dt.strftime('%Y-%m-%d %H:%M:%S'))
        df = pd.DataFrame(np.arange(len(date)),
                          columns=["step"],
                          index=date)
        df['timestamp'] = df.index.strftime('%Y-%m-%d %H:%M:%S')
        # df_slice = df.iloc[df.index.get_indexer(datetime.strptime(time_step_of_viz,\
        #                                                 '%Y-%m-%d %H:%M:%S'),method='nearest')]
        idx = df.index.get_indexer([pd.Timestamp(time_step_of_viz)], method='nearest').item()
        df_slice = df.iloc[idx:idx+1]
        step = df_slice['step'].item()
        time_stamp = df_slice['timestamp'].item()
        print('the closest time step to what is provided for vizualization ', time_step_of_viz,\
              ' is ', time_stamp)
        # load the data and get the max and min values of remppaed file for the taarget variable
        max_value = ds_source[source_nc_var_name].sel(time=time_stamp, method='nearest').max().item() # get the max of remapped
        min_value = ds_source[source_nc_var_name].sel(time=time_stamp, method='nearest').min().item() # get the min of remapped
        print('min: {}, max: {} for variable: {} in source nc file for the time step: {}'.format(\
            min_value, max_value, source_nc_var_name, time_stamp))
        # check if remapped file exists and check the time variables to source nc file
        if remapped_nc_exists:
            ds_remapped = xr.open_dataset(remapped_nc_name) # the remap of above
            # check if the times are identical in source and remapped
            if not ds_source[source_nc_var_time].equals(ds_remapped[remapped_nc_var_time]):
                sys.exit('The source and remapped files seems to have different time; make sure '+\
                         'the remapped files is from the same source file.')
            # update the max min value based on remapped
            max_value = ds_remapped[remapped_nc_var_name].sel(time=time_stamp, method='nearest').max().item() # get the max of remapped
            min_value = ds_remapped[remapped_nc_var_name].sel(time=time_stamp, method='nearest').min().item() # get the min of remapped
            print('min: {}, max: {} for variable: {} in remapped nc file for the time step: {}'.format(\
            min_value, max_value, remapped_nc_var_name, time_stamp))
            #
            shp_target = gpd.read_file(target_shp_name) # load the target shapefile
            if (min_lon is None) or (min_lat is None) or (max_lon is None) or (max_lat is None):
                min_lon, min_lat, max_lon, max_lat = shp_target.total_bounds
        # correct min and max if the are given
        if min_value_colorbar:
            min_value = min_value_colorbar
            print('min values for colorbar is provided as: ',min_value)
        if max_value_colorbar:
            max_value = max_value_colorbar
            print('max values for colorbar is provided as: ',max_value)
        # visualize
        fig, ax = plt.subplots(figsize=fig_size)
        fig.set_facecolor("white")
        if (self.case == 1 or self.case ==2) and show_source_flag:
            ds_source[source_nc_var_name].sel(time=time_stamp, method='nearest').plot.pcolormesh(x=source_nc_var_lon,
                                                                y=source_nc_var_lat,
                                                                add_colorbar=add_colorbar_flag,
                                                                ax = ax,
                                                                cmap=cmap,
                                                                vmin=min_value,
                                                                vmax=max_value,
                                                                alpha=alpha_source)
            colorbar_do_not_exists = False
        if self.case == 3 and show_source_flag:
            # dataframe
            df = pd.DataFrame()
            df ['ID'] = ds_source[source_nc_var_ID][:].values.astype(int)
            df ['value'] = ds_source[source_nc_var_name].sel(time=time_stamp, method='nearest') # assumes times is first
            df = df.sort_values(by=['ID'])
            df = df.reset_index(drop=True)
            # shapefile
            shp_source[source_shp_field_ID] = shp_source[source_shp_field_ID].astype(int)
            shp_source = shp_source[shp_source[source_shp_field_ID].isin(df['ID'])]
            shp_source = shp_source.sort_values(by=[source_shp_field_ID])
            shp_source = shp_source.reset_index(drop=True)
            # pass the values from datarame to geopandas and visuazlie
            shp_source ['value'] = df ['value']
            shp_source.plot(column='value',
                            edgecolor='k',
                            linewidth=linewidth_source,
                            ax=ax,
                            cmap=cmap,
                            vmin=min_value,
                            vmax=max_value,
                            alpha=alpha_source)
            if add_colorbar_flag:
                # provide the time and date and other parameters
                ax.set_title('time: '+time_stamp)
                ax.set_xlabel('longitude')
                ax.set_ylabel('latitude')
                norm = mpl.colors.Normalize(vmin=min_value, vmax=max_value)
                cbar = fig.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
                if 'units' in ds_source[source_nc_var_name].attrs.keys():
                    unit_name = ds_source[source_nc_var_name].attrs["units"]
                    cbar.ax.set_ylabel(source_nc_var_name+' ['+unit_name+']')
                else:
                    cbar.ax.set_ylabel(remapped_nc_var_name)
                colorbar_do_not_exists = False
        if source_shp_center_flag: # add points into the source such as centroid
            if source_shp_name:
                shp_source = gpd.read_file(source_shp_name)
            else:
                sys.exit('Source shapefile is not provided while source_shp_center_flag is \
                    set to true, provide source shapefile with lat and lon')
            shp_source_points = shp_source.copy()
            crs_org = shp_source_points.crs
            shp_source_points = shp_source_points.drop(columns=['geometry'])
            shp_points = self.make_shape_point(shp_source_points, source_shp_field_lon, source_shp_field_lat, crs=crs_org)
            shp_points.plot(color=source_shp_center_color, ax=ax)
        if remapped_nc_exists:
            if show_remapped_values_flag:
                show_target_shp_flag = False
            if show_target_shp_flag:
                shp_target.geometry.boundary.plot(color=None,edgecolor='k',linewidth = linewidth_remapped, ax = ax)
            if show_remapped_values_flag:
                # dataframe
                df = pd.DataFrame()
                df ['ID'] = ds_remapped[remapped_nc_var_ID][:].values.astype(int)
                df ['value'] = ds_remapped[remapped_nc_var_name].sel(time=time_stamp,method='nearest')
                df = df.sort_values(by=['ID'])
                df = df.reset_index(drop=True)
                # shapefile
                shp_target[target_shp_field_ID] = shp_target[target_shp_field_ID].astype(int)
                shp_target = shp_target[shp_target[target_shp_field_ID].isin(df['ID'])]
                shp_target = shp_target.sort_values(by=[target_shp_field_ID])
                shp_target = shp_target.reset_index(drop=True)
                # pass the values from datarame to geopandas and visuazlie
                shp_target ['value'] = df ['value']
                shp_target.plot(column='value',
                                edgecolor='k',
                                linewidth=linewidth_remapped,
                                ax=ax,
                                cmap=cmap,
                                vmin=min_value,
                                vmax=max_value,
                                alpha=alpha_remapped)
            if add_colorbar_flag and (colorbar_do_not_exists):
                # provide the time and date and other parameters
                ax.set_title('time: '+time_stamp)
                ax.set_xlabel('longitude')
                ax.set_ylabel('latitude')
                norm = mpl.colors.Normalize(vmin=min_value, vmax=max_value)
                cbar = fig.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
                if 'units' in ds_remapped[remapped_nc_var_name].attrs.keys():
                    unit_name = ds_remapped[remapped_nc_var_name].attrs["units"]
                    cbar.ax.set_ylabel(remapped_nc_var_name+' ['+unit_name+']')
                else:
                    cbar.ax.set_ylabel(remapped_nc_var_name)
            if target_shp_center_flag: # add points into the source such as centroid
                shp_target_points = shp_target.copy()
                crs_org = shp_target_points.crs
                shp_target_points = shp_target_points.drop(columns=['geometry'])
                shp_points = self.make_shape_point(shp_target_points, target_shp_field_lon, target_shp_field_lat, crs=crs_org)
                shp_points.plot(color=target_shp_center_color, ax=ax)
        #
        if min_lat and min_lon and max_lat and max_lon:
            ax.set_ylim([min_lat-margin,max_lat+margin])
            ax.set_xlim([min_lon-margin,max_lon+margin])
        # create the folder to save
        plt.tight_layout()
        if location_save_fig and fig_name:
            if not os.path.isdir(location_save_fig):
                os.makedirs(location_save_fig)
            plt.savefig(location_save_fig+fig_name, bbox_inches='tight')

