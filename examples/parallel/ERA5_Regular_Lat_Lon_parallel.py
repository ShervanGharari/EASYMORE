import multiprocessing
import os
import glob
from   easymore.easymore import easymore

def easymore_config():
    esmr = easymore() # initial the easymore
    #esmr.read_config_dict('config.txt') # read the configuration file
    esmr.case_name   = "ERA5_capitals_north_america"
    esmr.temp_dir    = "../temporary/"
    esmr.target_shp  = "../data/target_shapefiles/Capitals_point.shp"
    esmr.source_nc   = "../data/Source_nc_ERA5/ERA5_NA_*.nc"
    esmr.var_names   = ["airtemp"]
    esmr.var_lon     = "longitude"
    esmr.var_lat     = "latitude"
    esmr.var_time    = "time"
    esmr.output_dir  = "../output/"
    return esmr

def process_nc(nc_name):
    print("Processing", nc_name)
    print(str(os.getpid()))

    esmr = easymore_config()
    esmr.only_create_remap_csv = False # back to false/default in case if it reamins true (de)
    esmr.source_nc = nc_name # update the file name
    esmr.remap_csv = esmr.temp_dir+esmr.case_name +'_remapping.csv' # pass the remapping file name to skip remapping craetion
    esmr.nc_remapper() # create remap file

if __name__ == '__main__':

    # initialise easymore and read configuration from the file
    esmr = easymore_config()
    esmr.only_create_remap_csv = True # only crate the remapping file

    # get the nc file names
    nc_names = sorted(glob.glob(esmr.source_nc))
    # Create a multiprocessing.Pool with the desired number of processes
    num_processes = multiprocessing.cpu_count()  # Use the number of available CPU cores
    num_processes = min(len(nc_names), num_processes) # limit the worker if number of files is smaller
    pool = multiprocessing.Pool(processes=num_processes) # assign the number of workers

    # Use pool.map() to parallelize the for loop
    pool.map(process_nc, nc_names)

    # Close the pool to free up resources
    pool.close()
    pool.join()
