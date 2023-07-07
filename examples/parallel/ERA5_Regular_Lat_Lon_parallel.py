import multiprocessing
import os
from easymore.easymore import easymore

def process_nc(nc_name):
    print("Processing", nc_name)
    print(str(os.getpid()))

    esmr = easymore() # initialize the easymore object
    esmr.read_config_dict('config.txt') # read the config file
    esmr.only_create_remap_csv = False # back to false/default in case if it reamins true (de)
    esmr.source_nc = nc_name # update the file name
    esmr.remap_csv = esmr.temp_dir+esmr.case_name +'_remapping.csv' # pass the remapping file name to skip remapping craetion
    esmr.nc_remapper() # create remap file

if __name__ == '__main__':

    # initialise easymore and read configuration from the file
    esmr = easymore() # initial the easymore
    esmr.read_config_dict('config.txt') # read the configuration file
    esmr.only_create_remap_csv = True # only crate the remapping file
    esmr.nc_remapper() # create remap file

    # get the nc file names
    nc_names = sorted(glob.glob(esmr.source_nc))
    # Create a multiprocessing.Pool with the desired number of processes
    num_processes = multiprocessing.cpu_count()  # Use the number of available CPU cores
    num_processes = min(len(nc_names), num_processes) # limit the worker if number of files is lower
    pool = multiprocessing.Pool(processes=num_processes) # assign the number of workers

    # Use pool.map() to parallelize the for loop
    pool.map(process_nc, nc_names)

    # Close the pool to free up resources
    pool.close()
    pool.join()

