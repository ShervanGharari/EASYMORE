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
from   simpledbf    import Dbf5


class candex:

    def __init__(self):

        self.name_of_case              =  '' # name_of_case
        self.temporary_candex_folder   =  '' # temporary_candex_folder
        self.name_of_target_shp        =  '' # name_of_target_shp
        self.name_of_field_target_shp  =  [] # name_of_field_target_shp
        self.name_of_nc_files          =  '' # name_of_nc_files
        self.name_of_shp_for_nc_files  =  '' # name_of_nc_files
        self.name_of_field_target_lat_shp_for_nc_files  =  '' # name_of_nc_files
        self.name_of_field_target_lon_shp_for_nc_files  =  '' # name_of_nc_files
        self.name_of_var_name          =  '' # name_of_var_name
        self.name_of_var_lon           =  '' # name_of_var_lon
        self.name_of_var_lat           =  '' # name_of_var_lat
        self.name_of_var_time          =  '' # name_of_var_time
        self.name_of_var_ID            =  '' # name_of_nc_files
        self.name_of_nc_output_folder  =  '' # name_of_nc_files
        self.format_list               =  []
        self.fill_value_list           =  []
        self.name_of_remap_file        =  '' # name_of_nc_files
        self.authour_name              =  ''
        self.tolerance                 =  10**-5 # tolerance
        # self.box_flag                  =  True # box_flag; may not be used...
        # self.map_on_ID                 =  False # for future development for remapping on IDs only not supported and not recommended



    def run_candex(self):

        if self.name_of_remap_file == '':
            self.__check_candex_input()
            self.__check_target_shp() # check the target shapefile
            self.__check_source_nc() # check the netCDF file and their dimensions
            self.__NetCDF_SHP_lat_lon()
            self.__check_source_nc_shp()
            self.__intersection_shp()
            self.__create_remap()
        else:
            print('remap file is provided; candex will use this file and skip calculation of remapping')
            self.__check_source_nc() # check the netCDF file and their dimensions
        self.__target_nc_creation()

    def __check_candex_input (self):
        if self.temporary_candex_folder != '':
            if self.temporary_candex_folder[-1] != '/':
                sys.exit('the provided temporary folder for candex should end with (/)')
            if not os.path.isdir(self.temporary_candex_folder):
                os.mkdir(self.temporary_candex_folder)
        if self.name_of_nc_output_folder != '':
            if self.name_of_nc_output_folder[-1] != '/':
                sys.exit('the provided output folder for candex should end with (/)')
            if not os.path.isdir(self.name_of_nc_output_folder):
                os.mkdir(self.name_of_nc_output_folder)
        if self.temporary_candex_folder == '':
            print("No temporary folder is provided for candex; this will result in candex saving the files in the same directory as python script")
        if self.authour_name == '':
            print("no  author name is provide and the author name is changes to (author name)")
        if self.name_of_nc_output_folder == '':
            sys.exit('the provided folder for candex remapped netCDF output is missing; please provide that')
        if (len(self.name_of_var_name) != 1) and (len(self.format_list) != 1) and (len(self.fill_value_list) !=1):
            if (len(self.name_of_var_name) != len(self.fill_value_list)) and \
            (len(self.name_of_var_name) != len(self.format_list)) and \
            (len(self.format_list) == 1) and (len(self.fill_value_list) ==1):
                print('candex is given multiple varibales to be remapped but only on format and fill value'+\
                    'candex repeat the format and fill value for all the variables in output files')
                self.format_list     = self.format_list     * len(self.name_of_var_name)
                self.fill_value_list = self.fill_value_list * len(self.name_of_var_name)
            else:
                sys.exit('number of varibales and fill values and formats do not match')


    def __check_target_shp (self):
        # load the needed packages
        import geopandas as gpd
        from shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely

        # target shapefile is what we want the varibales to be remapped to; South Saskachewan River at Medicine Hat
        shp = gpd.read_file(self.name_of_target_shp)
        if (str(shp.crs).lower() != 'epsg:4326'):
            sys.exit('please project your shapefile to WGS84 (epsg:4326)')
        if (str(shp.crs).lower() == 'epsg:4326'): # check if the projection is WGS84 (or epsg:4326)
            print('candex detects that target shapefile is in WGS84 (epsg:4326)')

        if self.name_of_field_target_shp:
            if len(self.name_of_field_target_shp) == 1 or len(self.name_of_field_target_shp) == 3:
                print('candex detects that target IDs is provided with the name: ', self.name_of_field_target_shp[0])
                ID_values = np.array(shp[self.name_of_field_target_shp[0]])
                # here there should be a check for the uniqueness of the ID values.
                if len(ID_values) == len(np.unique(ID_values)):
                    print('candex detects that the IDs provided for shapes in shapefile are unique')
                if sum(ID_values) == sum(ID_values.astype(int)):
                    print('candex detects that the IDs provided for shapes in shapefile are integers')
                    shp['ID_t'] = shp[self.name_of_field_target_shp[0]]
                else:
                    print('candex detects that the IDs provided for shapes in shapefile are not integers and will add ID_t as integer')
                    shp['ID_t']  = np.arange(len(shp))+1
                if len(self.name_of_field_target_shp) == 3:
                    print('candex detects that target lat if provided with the name: ', self.name_of_field_target_shp[1])
                    print('candex detects that target lon if provided with the name: ', self.name_of_field_target_shp[2])
                    # here we should check that the lat lon are float
                    shp['lat_t'] = shp[self.name_of_field_target_shp[1]]
                    shp['lon_t'] = shp[self.name_of_field_target_shp[2]]
                elif len(self.name_of_field_target_shp) == 1:
                    print('candex detects that target lat/lon are not provided and will be calcuated')
                    # here we should check that the lat lon are float
                    shp['lat_t'] = shp.centroid.y # centroid lat from target
                    shp['lon_t'] = shp.centroid.x # centroid lon from target
        else:
            print('candex detects that no fields are provided for ID, lat, lon and will assign those by itself')
            shp['ID_t']  = np.arange(len(shp))+1
            shp['lat_t'] = shp.centroid.y # centroid lat from target
            shp['lon_t'] = shp.centroid.x # centroid lon from target


        detendted_points = False
        detendted_multipoints = False
        for index, _ in shp.iterrows():
            polys = shp.geometry.iloc[index] # get the shape
            if polys.geom_type.lower() == "point" or polys.geom_type.lower() == "points":
                detendted_points = True
                polys = polys.buffer(10**-5).simplify(10**-5)
                shp.geometry.iloc[index] = polys
            if polys.geom_type.lower() == "multipoint" or polys.geom_type.lower() == "multipoints":
                detendted_multipoints = True
                polys = polys.buffer(10**-5).simplify(10**-5)
                shp.geometry.iloc[index] = polys

        if detendted_points:
            print('candex detects point(s) as geometry of target shapefile and will apply small buffer to them')
        if detendted_multipoints:
            print('candex detected multipoint as geometry of target shapefile and will considere it as multipolygone')
            print('hence candex will provide the average of all the point in each multipoint')
            print('if you mistakenly have given poitns as multipoints please correct the target shapefile')

        # save the standard target shapefile
        print('candex will save standard shapefile for candex claculation as:')
        print(self.temporary_candex_folder+self.name_of_case+'_target_shapefile.shp')
        shp.to_file(self.temporary_candex_folder+self.name_of_case+'_target_shapefile.shp') # save

    def __check_source_nc (self):

        flag_do_not_match = False

        nc_names = glob.glob (self.name_of_nc_files)

        if not nc_names:
            sys.exit('candex detects no netCDF file; check the path to the soure netCDF files')
        else:
            ncid = nc4.Dataset(nc_names[0])
            var_dim = list(ncid.variables[self.name_of_var_name[0]].dimensions)
            lat_dim = list(ncid.variables[self.name_of_var_lat].dimensions)
            lon_dim = list(ncid.variables[self.name_of_var_lon].dimensions)
            lat_value = np.array(ncid.variables[self.name_of_var_lat])
            lon_value = np.array(ncid.variables[self.name_of_var_lon])


            # dimension checks based on the first netcdf file
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

            # for varibale lat
            for nc_name in nc_names:
                ncid = nc4.Dataset(nc_name)
                temp = list(ncid.variables[self.name_of_var_lat].dimensions)
                # fist check the length of the temp and lat_dim
                if len(temp) != len(lat_dim):
                    flag_do_not_match = True
                else:
                    for i in np.arange(len(temp)):
                        if temp[i] != lat_dim[i]:
                            flag_do_not_match = True
                temp = np.array(ncid.variables[self.name_of_var_lat])
                if np.sum(abs(lat_value-temp))>self.tolerance:
                    flag_do_not_match = True

            # for varibale lon
            for nc_name in nc_names:
                ncid = nc4.Dataset(nc_name)
                temp = list(ncid.variables[self.name_of_var_lon].dimensions)
                # fist check the length of the temp and lon_dim
                if len(temp) != len(lon_dim):
                    flag_do_not_match = True
                else:
                    for i in np.arange(len(temp)):
                        if temp[i] != lon_dim[i]:
                            flag_do_not_match = True
                temp = np.array(ncid.variables[self.name_of_var_lon])
                if np.sum(abs(lon_value-temp))>self.tolerance:
                    flag_do_not_match = True

            for var_name in self.name_of_var_name:
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

    def __check_source_nc_shp (self):


        if self.case == 3:

            # load the needed packages
            import geopandas as gpd
            from shapely.geometry import Polygon
            import shapefile # pyshed library
            import shapely

            nc_names = glob.glob (self.name_of_nc_files)
            ncid = nc4.Dataset(nc_names[0])

            # target shapefile is what we want the varibales to be remapped to; South Saskachewan River at Medicine Hat
            shp = gpd.read_file(self.name_of_shp_for_nc_files)
            if (str(shp.crs).lower() != 'epsg:4326'):
                sys.exit('please project your source shapefile and varibales in source nc files to WGS84 (epsg:4326)')
            if (str(shp.crs).lower() == 'epsg:4326'): # check if the projection is WGS84 (or epsg:4326)
                print('candex detects that source shapefile is in WGS84 (epsg:4326)')

            # get the lat/lon from source shapfile and nc files
            lat_shp = np.array(shp[self.name_of_field_target_lat_shp_for_nc_files]); lat_shp = lat_shp.astype(float)
            lon_shp = np.array(shp[self.name_of_field_target_lon_shp_for_nc_files]); lon_shp = lon_shp.astype(float)
            lat_nc  = np.array(ncid.variables[self.name_of_var_lat][:]);             lat_nc  = lat_nc.astype(float)
            lon_nc  = np.array(ncid.variables[self.name_of_var_lat][:]);             lon_nc  = lon_nc.astype(float)

            # check the length of the lat/lon from shapfile and nc file
            print('checking latitude and longitude values from source shapefile and nc file')
            print('this may take a while')
            if len (lat_nc) <= len (lat_shp):
                temp = list(np.floor(np.linspace(1, len (lat_nc), num=21)))
                for i in np.arange (len (lat_nc)):
                    if i in temp:
                        print('progress: ', i/temp[-1])
                    distance = ((lat_shp - lat_nc[i])**2 + (lon_shp - lon_nc[i])**2)**0.5
                    distance_smaller = np.array((distance < self.tolerance))
                    if sum (distance_smaller) > 1: # only one value
                        sys.exit('there is discripancies between the source nc and shapefile lat/lon; please check')
            else:
                temp = list(np.floor(np.linspace(1, len (lat_shp), num=21)))
                for i in np.arange (len (lat_shp)):
                    if i in temp:
                        print('progress: ', i/temp[-1])
                    distance = ((lat_nc - lat_shp[i])**2 + (lon_nc - lon_shp[i])**2)**0.5
                    distance_smaller = np.array((distance < self.tolerance))
                    if sum (distance_smaller) > 1: # only one value
                        sys.exit('there is discripancies between the source nc and shapefile lat/lon; please check')
            print('all the point in the source and ')


    def __NetCDF_SHP_lat_lon(self):
        import geopandas as gpd
        from shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
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

        nc_names = glob.glob (self.name_of_nc_files)
        var_name = self.name_of_var_name[0]


        # open the nc file to read
        ncid = nc4.Dataset(nc_names[0])

        # deciding which case
        # case #1 regular lat/lon
        if (len(ncid.variables[self.name_of_var_lon].dimensions)==1) and (len(ncid.variables[self.name_of_var_lon].dimensions)==1) and\
           (len(ncid.variables[self.name_of_var_name[0]].dimensions)==3):
            print('candex detects case 1 - regular lat/lon')
            self.case = 1
            # get the list of dimensions for the ncid sample varibale
            list_dim_name = list(ncid.variables[self.name_of_var_name[0]].dimensions)
            # get the location of lat dimensions
            location_of_lat = list_dim_name.index(list(ncid.variables[self.name_of_var_lat].dimensions)[0])
            locaiton_of_lon = list_dim_name.index(list(ncid.variables[self.name_of_var_lon].dimensions)[0])
            # det the dimensions of lat and lon
            len_of_lat = len(ncid.variables[self.name_of_var_lat][:])
            len_of_lon = len(ncid.variables[self.name_of_var_lon][:])
            if locaiton_of_lon > location_of_lat:
                lat = np.zeros([len_of_lat, len_of_lon])
                lon = np.zeros([len_of_lat, len_of_lon])
                for i in np.arange(len(ncid.variables[self.name_of_var_lon][:])):
                    lat [:,i] = ncid.variables[self.name_of_var_lat][:]
                for i in np.arange(len(ncid.variables[self.name_of_var_lat][:])):
                    lon [i,:] = ncid.variables[self.name_of_var_lon][:]
            else:
                lat = np.zeros([len_of_lon, len_of_lat])
                lon = np.zeros([len_of_lon, len_of_lat])
                for i in np.arange(len(ncid.variables[self.name_of_var_lon][:])):
                    lat [i,:] = ncid.variables[self.name_of_var_lat][:]
                for i in np.arange(len(ncid.variables[self.name_of_var_lat][:])):
                    lon [:,i] = ncid.variables[self.name_of_var_lon][:]
            # creating/saving the shapefile
            lat = np.array(lat).astype(float); lon = np.array(lon).astype(float)
            self.lat = lat; self.lon = lon
            self.__lat_lon_SHP(lat, lon)
        # case #2 rotated lat/lon
        if (len(ncid.variables[self.name_of_var_lat].dimensions)==2) and (len(ncid.variables[self.name_of_var_lon].dimensions)==2):
            print('candex detects case 2 - rotated lat/lon')
            self.case = 2
            lat = ncid.variables[self.name_of_var_lat][:,:]
            lon = ncid.variables[self.name_of_var_lon][:,:]
            # creating/saving the shapefile
            lat = np.array(lat).astype(float); lon = np.array(lon).astype(float)
            self.lat = lat; self.lon = lon
            self.__lat_lon_SHP(lat, lon)
        # case #3 1-D lat/lon and 2 data for irregulat shapes
        if (len(ncid.variables[self.name_of_var_lat].dimensions)==1) and (len(ncid.variables[self.name_of_var_lon].dimensions)==1) and\
           (len(ncid.variables[self.name_of_var_name[0]].dimensions)==2):
            print('candex detects case 3 - irregular lat/lon; shapefile should be provided')
            self.case = 3
            lat = ncid.variables[self.name_of_var_lat][:]
            lon = ncid.variables[self.name_of_var_lon][:]
            print(lat, lon)
            if self.name_of_var_ID  == '':
                print('candex detects that no varibale for ID of the source netCDF file; an arbitatiry ID will be provided')
                ID  =  np.arange(len(lat))+1 # pass arbitarary values
            else:
                ID = ncid.variables[self.name_of_var_ID][:]
            # creating/saving the shapefile
            lat = np.array(lat).astype(float); lon = np.array(lon).astype(float)
            self.lat = lat; self.lon = lon; self.ID = ID
            if self.name_of_shp_for_nc_files == '':
                sys.exit("no shapfile is provided for the source netCDF file, please provide the associated shapefile to the netCDF file")
            self.__lat_lon_SHP(lat, lon)


    def __lat_lon_SHP(self, lat, lon):
        import geopandas as gpd
        from shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
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
        if self.case == 1 or self.case == 2:
            if ((lat.shape[0] != lon.shape[0]) or (lat.shape[1] != lon.shape[1])):
                sys.exit("no shapfile is created, please provide the associated shapefile to the netCDF file")
        else:
            if (lat.shape[0] != lon.shape[0]):
                sys.exit("no shapfile is created, please provide the associated shapefile to the netCDF file")

        if self.case == 1 or self.case ==2:

            lat_lon_shape = lat.shape

            # name of the shapefile
            print('candex is creating the shapefile from the netCDF file and saving it here:')
            print(self.temporary_candex_folder+self.name_of_case+'_source_shapefile.shp')
            name_of_shp = self.temporary_candex_folder+self.name_of_case+'_source_shapefile.shp'

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

            shp_source = gpd.read_file(name_of_shp)
            shp_source = shp_source.set_crs("EPSG:4326") # set the projection to WGS84

        else:
            name_of_shp = self.name_of_shp_for_nc_files
            shp_source = gpd.read_file(name_of_shp)
            shp_source ['lat_s'] = shp_source [self.name_of_field_target_lat_shp_for_nc_files].astype(float)
            shp_source ['lon_s'] = shp_source [self.name_of_field_target_lon_shp_for_nc_files].astype(float)
            shp_source ['ID_s']  = np.arange(len(shp_source))

        if max (shp_source['lon_s']) > 180 and min (shp_source['lon_s']) > 0 and max (shp_source['lon_s']) < 360:

            print('candex decides the netCDF file has longtitude values of 0 to 360; creating the extended')

            shp1 = shp_source [shp_source['lon_s'] <= 180]
            shp2 = shp_source [shp_source['lon_s'] >  180]

            if not shp1.empty:
                shp1.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileA.shp')
                shp1 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileA.shp')
                temp = shp1

            if not shp2.empty:
                shp2.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileB.shp')
                shp2 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileB.shp')
                shp3 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileB.shp')
                # loop change the geometry
                for index, _ in shp3.iterrows():
                    polys = shp3.geometry.iloc[index] # get the shape
                    polys = shapely.affinity.translate(polys, xoff=-360.0, yoff=0.0, zoff=0.0)
                    shp3.geometry.iloc[index] = polys
                shp3.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileC.shp')
                shp3 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileC.shp')
                temp = gpd.GeoDataFrame( pd.concat( [shp3,shp2] , ignore_index=True  ) )
                if not shp1.empty:
                    temp = gpd.GeoDataFrame( pd.concat( [temp,shp1] , ignore_index=True )  )


            temp.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefile_expanded.shp')
            print('candex saved the expanded shapefile at:')
            print(self.temporary_candex_folder+self.name_of_case+'_source_shapefile_expanded.shp')


        # the netcdf file has values between -180 to 180
        elif min (shp_source['lon_s']) > -180 and max (shp_source['lon_s']) < 180:

            print('candex decides the netCDF file has longtitude values of -180 to 180; creating the extended')

            shp1 = shp_source [shp_source['lon_s'] >   0]
            shp2 = shp_source [shp_source['lon_s'] <=  0]

            if not shp1.empty:
                shp1.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileA.shp')
                shp1 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileA.shp')
                temp = shp1

            if not shp2.empty:
                shp2.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileB.shp')
                shp2 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileB.shp')
                shp3 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileB.shp')
                # loop change the geometry
                for index, _ in shp3.iterrows():
                    polys = shp3.geometry.iloc[index] # get the shape
                    polys = shapely.affinity.translate(polys, xoff=+360.0, yoff=0.0, zoff=0.0)
                    shp3.geometry.iloc[index] = polys
                shp3.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileC.shp')
                shp3 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefileC.shp')
                temp = gpd.GeoDataFrame( pd.concat( [shp3,shp2] , ignore_index=True  ) )
                if not shp1.empty:
                    temp = gpd.GeoDataFrame( pd.concat( [temp,shp1] , ignore_index=True )  )

            temp.to_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefile_expanded.shp')
            print('candex saved the expanded shapefile at:')
            print(self.temporary_candex_folder+self.name_of_case+'_source_shapefile_expanded.shp')

        else:
            print('candex cannot decide about the lat and lon of the shapefiles')



    def __intersection_shp(self):
        import geopandas as gpd
        from shapely.geometry import Polygon
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
        shp1: geo data frame, shapefile 1
        shp2: geo data frame, shapefile 2

        Returns
        -------
        result: a geo data frame that includes the intersected shapefile and area, percent and normalized percent of each shape
        elements in another one
        """
        # Calculating the area of every shapefile (both should be in degree or meters)

        # shp target
        shp_1 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_target_shapefile.shp')
        # shp source
        shp_2 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_source_shapefile_expanded.shp')

        # get the maximume and minumue of the shp1 for the
        min_lon, min_lat, max_lon, max_lat = shp_1.total_bounds
        shp_2 = shp_2 [shp_2['lon_s'] < max_lon+2]
        shp_2 = shp_2 [shp_2['lon_s'] > min_lon-2]
        shp_2 = shp_2 [shp_2['lat_s'] < max_lat+2]
        shp_2 = shp_2 [shp_2['lat_s'] > min_lat-2]
        if shp_2.empty:
            sys.exit("somthing is wrong! candex cannot find the ovrlap between the target bounding box and source (netCDF) shapefiles")
        shp_2.to_file(self.temporary_candex_folder+self.name_of_case+'_bounded_source.shp') # save the intersected files
        shp_2 = gpd.read_file(self.temporary_candex_folder+self.name_of_case+'_bounded_source.shp') # save the intersected files

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
        result = self.__spatial_overlays (shp_1, shp_2, how='intersection')

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

        # rename dictionary
        dict_rename = {'S_1_ID_t' : 'ID_t',
                       'S_1_lat_t': 'lat_t',
                       'S_1_lon_t': 'lon_t',
                       'S_2_ID_s' : 'ID_s',
                       'S_2_lat_s': 'lat_s',
                       'S_2_lon_s': 'lon_s',
                       'AP1N'     : 'weight'}
        result = result.rename(columns=dict_rename) # rename fields
        result = result.sort_values(by=['ID_t']) # sort based on ID_t
        result.to_file(self.temporary_candex_folder+self.name_of_case+'_intersected_shapefile.shp') # save the intersected files


    def __spatial_overlays(self, df1, df2, how='intersection', reproject=True):
        import geopandas as gpd
        from shapely.geometry import Polygon
        import shapefile # pyshed library
        import shapely
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

    def __create_remap(self):
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

        # cell 2: indexing of the source lat/lon to row and colomns in nc file
        remap_df = Dbf5(self.temporary_candex_folder+self.name_of_case+'_intersected_shapefile.dbf') # load dbf
        remap_df = remap_df.to_dataframe()

        # create the np arrays
        rows = np.zeros(len(remap_df['lat_s']))
        cols = np.zeros(len(remap_df['lon_s']))
        lat_source = np.array(remap_df['lat_s'])
        lon_source = np.array(remap_df['lon_s'])

        # loop to find the rows and colomns
        for i in np.arange(len(lat_source)):
            lat_lon_value_diff = abs(self.lat - lat_source[i])+abs(self.lon - lon_source[i])
            if self.case == 1 or self.case == 2:
                row, col = np.where(lat_lon_value_diff == np.min(lat_lon_value_diff))
            if self.case == 3:
                row = np.where(lat_lon_value_diff == np.min(lat_lon_value_diff))
                col = row
            rows [i] = row[0]
            cols [i] = col[0]

        remap_df['rows'] = rows
        remap_df['cols'] = cols

        # save remap_df as csv for future use
        remap_df.to_csv(self.temporary_candex_folder+self.name_of_case+'_remapping.csv')
        self.name_of_remap_file = self.temporary_candex_folder+self.name_of_case+'_remapping.csv'


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

        remap = pd.read_csv(self.name_of_remap_file)
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
        nc_names = glob.glob(self.name_of_nc_files)
        nc_names = sorted(nc_names)

        for nc_name in nc_names:

            # get the time unit and time var from source
            ncids = nc4.Dataset(nc_name)
            if 'units' in ncids.variables[self.name_of_var_time].ncattrs():
                time_unit = ncids.variables[self.name_of_var_time].units
            if 'calendar' in ncids.variables[self.name_of_var_time].ncattrs():
                time_cal = ncids.variables[self.name_of_var_time].calendar
            time_var = ncids[self.name_of_var_time][:]
            self.length_of_time = len(time_var)
            target_date_times = nc4.num2date(time_var,units = time_unit,calendar = time_cal)
            target_name = self.name_of_nc_output_folder + self.name_of_case + '_remapped_' + target_date_times[0].strftime("%Y-%m-%d-%H-%M-%S")+'.nc'
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
                time_varid.long_name = self.name_of_var_time
                time_varid.units = time_unit  # e.g. 'days since 2000-01-01 00:00' should change accordingly
                time_varid.calendar = time_cal
                time_varid.standard_name = self.name_of_var_time
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
                ncid.source = 'Case: ' +self.name_of_case + '; remapped by script from library of Shervan Gharari (https://github.com/ShervanGharari/candex).'


                # write varibales
                for i in np.arange(len(self.name_of_var_name)):

                    var_value  = self.__weighted_average( nc_name,
                                                          target_date_times,
                                                          self.name_of_var_name[i],
                                                          remap)

                    # Variables writing
                    varid = ncid.createVariable(self.name_of_var_name[i], self.format_list[i], ('time','ID' ), fill_value = self.fill_value_list[i])
                    varid [:] = var_value
                    # Pass attributes
                    if 'long_name' in ncids.variables[self.name_of_var_name[i]].ncattrs():
                        varid.long_name = ncids.variables[self.name_of_var_name[i]].long_name
                    if 'units' in ncids.variables[self.name_of_var_name[i]].ncattrs():
                        varid.units = ncids.variables[self.name_of_var_name[i]].units

                # reporting
                print('Ended   at date and time '+str(datetime.now()))



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
