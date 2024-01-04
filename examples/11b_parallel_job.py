# # An example of using EASYMORE with source netCDF files in regular Latitude and Longitude with missing values for a few grids and time steps
# ## Remapping of ERA5 to subbasins of South Saskatchewan River at Medicine Hat, Alberta, Canada.

# In[ ]:


# loading EASYMORE
#from easymore import easymore # for version 1 and below
from easymore import Easymore # for version 2 and above

# initializing EASYMORE object
# esmr = easymore() # for version 1 and below
esmr = Easymore() # for version 2 and above

# specifying EASYMORE objects
# name of the case; the temporary, remapping and remapped file names include case name
esmr.case_name                = 'ERA5_Medicine_Hat_parallel_job'
# temporary path that the EASYMORE generated GIS files and remapped file will be saved
esmr.temp_dir                 = './temporary/'
# name of target shapefile that the source netcdf files should be remapped to
esmr.target_shp               = './data/target_shapefiles/South_Saskatchewan_MedicineHat.shp'
esmr.target_shp_ID            = 'COMID' # if not provided easymore give ID according to shape order in shapefile
esmr.target_shp_lat           = 'lat' # if not provided the easymore provides lat from shape centroid
esmr.target_shp_lon           = 'lon' # if not provided the easymore provides lon from shape centroid
# name of netCDF file(s); multiple files can be specified with *
esmr.source_nc                = './data/Source_nc_ERA5/ERA5_NA_*.ncNaN'
# for complex issue with NaN this can be uncommented
# esmr.source_nc                = '../data/Source_nc_ERA5/ERA5_NA_*.ncNaN'
# name of variables from source netCDF file(s) to be remapped
esmr.var_names                = ['airtemp','pptrate']
# rename the variables from source netCDF file(s) in the remapped files;
# it will be the same as source if not provided
esmr.var_names_remapped       = ['temperature','precipitation']
# name of variable longitude in source netCDF files
esmr.var_lon                  = 'longitude'
# name of variable latitude in source netCDF files
esmr.var_lat                  = 'latitude'
# name of variable time in source netCDF file; should be always time
esmr.var_time                 = 'time'
# location where the remapped netCDF file will be saved
esmr.output_dir               = './output/'
# format of the variables to be saved in remapped files,
# if one format provided it will be expanded to other variables
esmr.format_list              = ['f4']
# fill values of the variables to be saved in remapped files,
# if one value provided it will be expanded to other variables
esmr.fill_value_list          = ['-9999.00']
# if required that the remapped values to be saved as csv as well
esmr.save_csv                 = True
esmr.complevel                = 9
# if set to true it will remove the shapes that do not intersect in the remapped files instead of reporting them with NaN values
esmr.skip_outside_shape       = False
# parallel flag and number of cpus
#esmr.parallel                 = True # can be set to True but it will turn true inside job
#esmr.numcpu                   = 10 # can be specify but will not be used and instead job cpu number will be used

# execute EASYMORE
esmr.nc_remapper()


# ------------
# ------------
# # Visualization of the source and result using easymore functionality

# In[ ]:


# loading EASYMORE
#from easymore import easymore # for version 1 and below
from easymore import Easymore # for version 2 and above

# initializing EASYMORE object
# esmr = easymore() # for version 1 and below
esmr = Easymore() # for version 2 and above

# define the source, and target files and parameters
case_name                  = 'ERA5_Medicine_Hat_parallel_job'
source_nc_name             = './data/Source_nc_ERA5/ERA5_NA_19790101.ncNaN'
source_nc_var_lon          = 'longitude'
source_nc_var_lat          = 'latitude'
source_nc_var_time         = 'time'
source_nc_var_name         = 'airtemp'
remapped_nc_name           = './output/ERA5_Medicine_Hat_parallel_job_remapped_ERA5_NA_19790101.ncNaN'
remapped_nc_var_name       = 'temperature'
remapped_nc_var_ID         = 'ID'
remapped_nc_var_time       = 'time'
time_step_of_viz           = '1979-01-01 3:00:00'
folder_save_fig            = './fig/'
target_shp_name            = './data/target_shapefiles/South_Saskatchewan_MedicineHat.shp'
target_shp_field_ID        = 'COMID'
cmap                       = 'viridis'
linewidth_remapped         = 1
margin                     = 0.1
font_size                  = 40
fig_size                   = (30,20)

# the source nc file
esmr.nc_vis(source_nc_name             = source_nc_name,
            source_nc_var_lon          = source_nc_var_lon,
            source_nc_var_lat          = source_nc_var_lat,
            source_nc_var_time         = source_nc_var_time,
            source_nc_var_name         = source_nc_var_name,
            time_step_of_viz           = time_step_of_viz,
            location_save_fig          = folder_save_fig,
            fig_name                   = case_name+'_1.png',
            cmap                       = cmap,
            font_size                  = font_size,
            fig_size                   = fig_size)


# the source nc file zoom to target shapefile without showing the remapped variables
esmr.nc_vis(source_nc_name             = source_nc_name,
            source_nc_var_lon          = source_nc_var_lon,
            source_nc_var_lat          = source_nc_var_lat,
            source_nc_var_time         = source_nc_var_time,
            source_nc_var_name         = source_nc_var_name,
            remapped_nc_name           = remapped_nc_name,
            remapped_nc_var_name       = remapped_nc_var_name,
            remapped_nc_var_ID         = remapped_nc_var_ID,
            remapped_nc_var_time       = remapped_nc_var_time,
            time_step_of_viz           = time_step_of_viz,
            location_save_fig          = folder_save_fig,
            target_shp_name            = target_shp_name,
            linewidth_remapped         = linewidth_remapped,
            show_target_shp_flag       = True,
            fig_name                   = case_name+'_2.png',
            cmap                       = cmap,
            margin                     = margin,
            font_size                  = font_size,
            fig_size                   = fig_size)


# the source nc file zoom to target shapefile with showing the remapped variables
esmr.nc_vis(source_nc_name             = source_nc_name,
            source_nc_var_lon          = source_nc_var_lon,
            source_nc_var_lat          = source_nc_var_lat,
            source_nc_var_time         = source_nc_var_time,
            source_nc_var_name         = source_nc_var_name,
            remapped_nc_name           = remapped_nc_name,
            remapped_nc_var_name       = remapped_nc_var_name,
            remapped_nc_var_ID         = remapped_nc_var_ID,
            remapped_nc_var_time       = remapped_nc_var_time,
            time_step_of_viz           = time_step_of_viz,
            location_save_fig          = folder_save_fig,
            target_shp_name            = target_shp_name,
            target_shp_field_ID        = target_shp_field_ID,
            linewidth_remapped         = linewidth_remapped,
            show_target_shp_flag       = True,
            show_remapped_values_flag  = True,
            fig_name                   = case_name+'_3.png',
            cmap                       = cmap,
            margin                     = margin,
            font_size                  = font_size,
            fig_size                   = fig_size)



# merge the two figures into one
from PIL import Image, ImageFont, ImageDraw
image_list = [folder_save_fig+case_name+'_2.png' , folder_save_fig+case_name+'_3.png']
image_list_cropped = [folder_save_fig+case_name+'_cropped_2.png' , folder_save_fig+case_name+'_cropped_3.png']


# crop if needed, can be commneted
im = Image.open(image_list[0])
width, height = im.size
crop_area = (0, 0, width*0.87, height) # (left, upper, right, lower)
cropped_image = im.crop(crop_area)
cropped_image.save(image_list_cropped[0])
#
im = Image.open(image_list[1])
width, height = im.size
crop_area = (width*0.0622, 0, width, height) # (left, upper, right, lower)
cropped_image = im.crop(crop_area)
cropped_image.save(image_list_cropped[1])

# merging
images = [Image.open(x) for x in image_list_cropped]
widths, heights = zip(*(i.size for i in images))
total_width = sum(widths)
max_height = max(heights)
new_im = Image.new('RGB', (total_width, max_height))
x_offset = 0
for im in images:
    new_im.paste(im, (x_offset,0))
    x_offset += im.size[0]
new_im.save(folder_save_fig+case_name+'.png')
new_im.save(folder_save_fig+'ERA5_Medicine_Hat_parallel_job.png')
new_im.show()
