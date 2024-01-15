"""
Common functions for general tasks such as conversion of a csv file to nc for future
manupulaiton
"""


import xarray as xr
import numpy as np
import glob
import os
import sys

from typing import (
    Optional,
    Dict,
    Tuple,
    Union,
    Sequence,
)

import xarray as xr
import glob
import sys
from   easymore import Easymore
import pandas as pd
import pint_xarray
import pint
import warnings
import cdo


class Utility:
    
    def drop_vars_with_dims(ds,
                            dims_to_drop):
        """
        Remove variables from xarray dataset that have specific dimensions.
    
        Parameters:
        -----------
        ds : xarray.Dataset
            Input xarray Dataset.
        dims_to_drop : list, optional
            List of dimensions to drop, by default [].
    
        Returns:
        --------
        xarray.Dataset
            Returns an xarray Dataset with variables removed based on provided dimensions.
        """
        # Get variables and their corresponding dimensions
        variables = list(ds.data_vars.keys())
        var_dims = [set(ds[var].dims) for var in variables]
    
        # Identify variables to drop based on provided dimensions
        vars_to_drop = [variables[i] for i, dims in enumerate(var_dims) if any(dim in dims_to_drop for dim in dims)]
    
        # Drop identified variables
        ds = ds.drop_vars(vars_to_drop)
    
        return ds
    
    
    def keep_vars_with_dims(ds,
                            dims_to_keep):
        """
        Keep variables in xarray dataset that have specific dimensions.
    
        Parameters:
        -----------
        ds : xarray.Dataset
            Input xarray Dataset.
        dims_to_keep : list, optional
            List of dimensions to keep, by default [].
    
        Returns:
        --------
        xarray.Dataset
            Returns an xarray Dataset with variables retained based on provided dimensions.
        """
        # Get variables and their corresponding dimensions
        variables = list(ds.data_vars.keys())
        var_dims = [set(ds[var].dims) for var in variables]
    
        # Identify variables to keep based on provided dimensions
        vars_to_keep = [variables[i] for i, dims in enumerate(var_dims) if any(dim in dims_to_keep for dim in dims)]
    
        # Keep identified variables
        ds = ds.drop_vars([var for var in variables if var not in vars_to_keep])
    
        return ds
    
    
    def vars_to_keep(ds,
                     vars_to_keep):
        """
        Extract specific variables along with their related dimensions from xarray dataset.
    
        Parameters:
        -----------
        ds : xarray.Dataset
            Input xarray Dataset.
        vars_to_keep : list, optional
            List of variables to keep, by default [].
    
        Returns:
        --------
        xarray.Dataset
            Returns an xarray Dataset containing only the specified variables and their related dimensions.
        """
        # Filter variables based on the provided list
        filtered_vars = [var for var in vars_to_keep if var in ds.data_vars]
    
        # Extract dimensions related to the specified variables
        related_dims = set()
        for var in filtered_vars:
            related_dims.update(ds[var].dims)
    
        # Extract variables and related dimensions
        filtered_ds = ds[filtered_vars].copy()
        filtered_ds = filtered_ds.assign_coords({dim: ds[dim] for dim in related_dims if dim in ds.coords})
    
        return filtered_ds
    
    
    def sorted_subset(ds,
                      ids,
                      mapping = {'var_id':'ID','dim_id':'ID'},
                      order_of_ids = None):
        
        
        
        
        # check if the ids are unique and different
        if len(np.unique(ids)) != len(ids):
            raise ValueError("ids array contains non-unique values.")
        
        # get the var and ID
        var_id = mapping.get('var_id')
        dim_id = mapping.get('dim_id')
        
        # 
        if order_of_ids is not None:
            
            # 
            if len(ids) != len(order_of_ids):
                raise ValueError("ids and order_of_ids must be the same length")
            if len(np.unique(order_of_ids)) != len(order_of_ids):
                raise ValueError("order od ids are not unique")
                
            # Find indices of 'ids' existing in 'var_id'
            indices = np.where(np.in1d(ids, ds[var_id][:]))[0]  # Using 0 as a placeholder, change it based on your requirement
    
            # Subset 'order_of_ids' based on the indices from 'ids' found in 'X'
            ids = np.array(ids)[indices]
            order_of_ids = np.array(order_of_ids)[indices]
            
            #
            print(ids, order_of_ids)
        
        # find the index in var_id and rearrange the nc file on that dimension
        idx = np.where(np.in1d(ds[var_id][:], ids))[0] # remove the order ?
        # idx = [i for i, val in enumerate(ds[var_id].values) if val in order_ids] # keep the order ?
        ds = ds.isel({dim_id:idx})
        
        if order_of_ids is not None:
                
            # Set a coordinate for sorting
            ds.coords['sorted_coord'] = xr.DataArray(order_of_ids, dims=dim_id, name='sorted_coord')
    
            # Sort the dataset along the specified coordinate
            ds = ds.sortby('sorted_coord')
    
            # drop sorted_coord
            ds = ds.drop_vars('sorted_coord')
        
        # return ds
        return ds
    
        
    import xarray as xr
    import numpy as np
    
    def sum_dim_id(ds, weight=None, dims={'dim_time':'time','dim_id': 'id'}):
        """
        Sum values for variables along the 'time' axis in an xarray dataset.
        If weight is provided, normalize it to sum up to one and multiply each variable by the normalized weight for each 'id' dimension.
    
        Parameters:
        -----------
        ds : xarray.Dataset
            Input xarray Dataset.
        weight : numpy.ndarray or None, optional
            Weight array to multiply each variable with for each 'id' dimension, by default None.
        dims : dict, optional
            Dictionary containing dimension names, by default {'dim_time': 'time', 'dim_id': 'id'}.
    
        Returns:
        --------
        xarray.Dataset
            Returns an xarray Dataset containing the summed values for each variable along the 'time' axis.
        """
        import copy
        ds = copy.deepcopy(ds)
        
        #
        dim_time = dims.get('dim_time')
        dim_id = dims.get('dim_id')
    
        # Check if 'id' dimension exists in the dataset
        if dim_id not in ds.dims:
            raise ValueError("Dimension 'id' does not exist in the dataset.")
    
        if weight is not None:
            if len(weight) != len(ds[dim_id]):
                raise ValueError("Weight length does not match the 'id' dimension in the dataset.")
    
            # Normalize weight values to sum up to one
            weight_sum = np.sum(weight)
            weight_normalized = weight / weight_sum
        else:
            weight_normalized = np.ones([1, len(ds[dim_id])]).flatten()
    
        # Iterate through the Dataset's variables
        for var_name in ds.variables:
            # Check if the variable has both 'time' and 'id' dimensions
            if dim_id in ds[var_name].dims and dim_time in ds[var_name].dims:
        
                # Find the index of the dimension named 'ID'
                id_dim_index = ds[var_name].dims.index(dim_id)
    
                # Get the length of the 'ID' dimension
                id_dim_length = len(ds[dim_id])
    
                # Create an array of ones from 1 to the length of the 'ID' dimension
                weight_arr = xr.DataArray(data=weight_normalized, dims='ID')
                weight_arr_nD = ds[var_name].copy()
    
                # Loop over the 'ID' dimension and fill it with the values from the 'ones_array'
                for idx in range(id_dim_length):
                    weight_arr_nD.loc[{ds[var_name].dims[id_dim_index]: idx}] = weight_arr[idx]
                
                #
                ds[var_name][:] = ds[var_name][:]*np.array(weight_arr_nD.values)
                ds[var_name] = ds[var_name].sum(dim=dim_id)
    
        return ds
        
        
    def reorder_output(file_name,
                       order_ids,
                       var_id,
                       dim_id,
                       var_time,
                       dim_time,
                       var_to_keep = None,
                       sum_flag = False,
                       save_reordered = False,
                       reorder_pre = 'reorder_',
                       output_folder = 'reorder'):
        
        """reorder a netcdf file based on an ID dim and variable
    
        Parameters
        ----------
        file_name : str
            The path to input forcing file(s). Can be specify with *.
        order_ids : Sequence[float]
            A sequence of ID that the nc will be reordered against.
        var_id : str
            Name of varibale that includes the original nc file IDs.
        dim_id : str
            Dimension of var_id.
        var_time : str
            Name of variable time.
        dim_time: Dict[str, str], optional
            Dimention of variable time.
        var_to_keep: list[str], optional
            collection of varibales to be included in reordered nc file(s).
        sum_flag: logical[], optional
            if set to true the values will be sum of all time series
        save_reordered: logical[], optional
            Flag to save the xarray object as nc file(s). If multiple file is 
            provided it will be set to True internally.
        reorder_pre: str, optional
            String that will added to the beginning of the saved reordered files
        output_folder: str, optional
            The path that the reorder file will be saved at.
    
        Returns
        -------
        xarray.Dataset
            Returns a reordered xarray.Dataset.
            If more files are identified, None.
    
        [FIXME]: The merge functionality could be added in the future if needed. 
        Currently the suggestion is to merge file using cdo mergetime outside of
        this function before or after reordering.
        """
        
        #
        files = sorted(glob.glob(file_name))
        
        # check if files are not empty
        if not files:
            sys.exit('no file is identified, check the input file name')
        
        if len(files)>1:
            print('The number of files passed to function is larger than 1. '+\
                  'The function output will be set to None. '+\
                  'The reorder files are going to be set written in '+\
                  output_folder+' folder with prefix of '+reorder_pre)
            print('It is suggested to use packages such as xarray or cdo to '+\
                  'merge the generated nc files if needed. Examples are: \n'+\
                  'cdo mergetime input*.nc merged.nc # example of directly using cdo librarty \n'+\
                  'or \n'+\
                  'import cdo # binary required \n'+\
                  'cdo_obj = cdo.Cdo()  # CDO object \n'+\
                  'ds = cdo_obj.mergetime(input=input*.nc, returnXArray=variables)  # Mergeing')
            save_reordered = True
    
        for file in files:
                  
            # open the nc file
            ds = xr.open_dataset(file)
            
            # drop unecessarily variables that are identified
            if not var_to_keep is None:
                variables_to_drop = [var for var in ds.variables if var not in var_to_keep]
                variables_to_drop = [elem for elem in variables_to_drop if elem not in [var_id,var_time]]
                print(variables_to_drop)
                ds = ds.drop_vars(variables_to_drop)
            
            # find the index in var_id and rearrange the nc file on that dimension
            idx = np.where(np.in1d(np.array(ds[var_id][:]), order_ids))[0]
            ds = ds.isel({dim_id:idx})
            
            # sum the reorder on dimension id;
            if sum_flag:
                # Iterate through the Dataset's variables
                for var_name in ds.variables:
                    # Check if the variable has both 'time' and 'segid' dimensions
                    if dim_time in ds[var_name].dims and dim_id in ds[var_name].dims:
                        ds[var_name]=ds[var_name].sum(dim=dim_id)
                    elif not dim_time in ds[var_name].dims:
                        ds = ds.drop_vars(var_name)
            
            # Save the rearranged dataset
            if save_reordered:
                if not os.path.isdir(output_folder):
                    os.makedirs(output_folder)
                file_name_to_save = os.path.join(output_folder, reorder_pre+os.path.basename(file))
                if os.path.isfile(file_name_to_save):
                    os.remove(file_name_to_save)
                ds.to_netcdf(file_name_to_save)
            
            # close the oppened file
            ds.close()
        
        if len(files)>1:
            ds = None
        
        # return
        return ds
    
    
    from typing import Dict, Optional
    import xarray as xr
    
    def convert_units(ds: xr.Dataset,
                      units: Dict[str, str] = None,
                      unit_registry: pint.UnitRegistry = None,
                      to_units: Optional[Dict[str, str]] = None) -> xr.Dataset:
        """Converts units of variables in xarray Dataset using Pint.
    
        Parameters
        ----------
        ds : xarray.Dataset
            Input xarray Dataset.
        units : Dict[str, str]
            A dictionary mapping variable names to their units.
        unit_registry : pint.UnitRegistry, optional
            A Pint unit registry for converting units, by default None.
        to_units : Dict[str, str], optional
            A dictionary mapping variable names to target units for conversion,
            by default None.
    
        Returns
        -------
        xarray.Dataset
            Returns an xarray.Dataset with converted units.
    
        Raises
        ------
        ValueError
            If any variable defined in `units` cannot be found in `ds`.
        """
        # check to see if all the keys included in the `units` dictionary are found inside the `ds`
        for k in units:
            if k not in ds:
                raise ValueError(f"item {k} defined in `units` cannot be found in `ds`")
    
        # if all elements of `ds` not found in `units`, assign them to None
        for v in ds.variables:
            if v not in units:
                units[v] = None
    
        # assign the units
        ds = ds.pint.quantify(units=units, unit_registry=unit_registry)
    
        # if `to_units` is defined, convert to the specified units
        if to_units:
            ds = ds.pint.to(units=to_units)
    
        # dequantify the units
        ds = ds.pint.dequantify()
    
        return ds
    
    
    def mergetime_nc_files(path: str, variables: Sequence[str]) -> xr.Dataset:
        """Merges multiple forcing files into a single xarray Dataset.
    
        Parameters
        ----------
        path : str
            The path to input forcing files.
        variables : Sequence[str]
            A sequence of variable names to be included in the output file.
    
        Returns
        -------
        xarray.Dataset
            Returns an xarray.Dataset containing the merged data.
    
        Notes
        -----
        The function merges all the input forcing files into a single NetCDF file
        using CDO and returns an xarray.Dataset.
        """
        cdo_obj = cdo.Cdo()  # CDO object
        ds = cdo_obj.mergetime(input=path, returnXArray=variables)  # Merging
        return ds
    
    
    def _set_min_values(
        ds: xr.Dataset,
        min_values: Dict[str, float],
    ) -> xr.Dataset:
        '''Setting minimum values (values of the `min_values` 
        dictionary) for different variables (keys of the 
        `min_values` dictionary).
        
        Parameters
        ----------
        ds : xarray.Dataset
            Dataset object of interest
        min_values : dict
            a dictionary with keys corresponding to any of the `ds`
            variables, and values corresponding to the minimums
    
        Returns
        -------
        ds : xarray.Dataset
            Dataset with minimum values set
        '''
        # make a copy of the dataset
        ds = ds.copy()
    
        # set the minimum values
        for var, val in min_values.items():
            ds[var] = xr.where(ds[var] <= val, val, ds[var])
    
        return ds
    
    
    def _fill_na_ds(
        ds: xr.Dataset,
        na_values: Dict[str, float],
    ) -> xr.Dataset:
        '''Replacing NA values within an xarray.Dataset
        object for different DataAarrays (corresponding to
        the keys of the `na_values`) with the corresponding
        values of `na_values`.
        '''
        # make a copy of the dataset
        ds = ds.copy()
    
        # replacing NAs
        for var, val in na_values.items():
            ds[var] = xr.where(ds[var] == np.nan, val, ds[var])
    
        return ds
    
    
    def calendar_no_leap_to_standard (data):
    
        # read start and end time and date from calendar no leap
        start_date = data.time.values[0].strftime("%Y-%m-%d %H:%M:%S")
        end_date = data.time.values[-1].strftime("%Y-%m-%d %H:%M:%S")
    
        # standard time
        standard_date = xr.cftime_range(start=start_date, end=end_date, freq='D', calendar='standard')
        standard_date_list = [str(date) for date in standard_date if not (date.year%4 == 0 and date.month == 2 and date.day == 29)]
    
        # return from standard_date_list
        new_time = [DatetimeGregorian.strptime(date, '%Y-%m-%d %H:%M:%S') for date in standard_date_list]
    
        # assign the new time that is standard without leap days as time
        data = data.assign_coords(time=new_time)
    
        # Extend the time dimension to include the standard calendar dates
        data = data.reindex(time=standard_date, method='pad')
        
        # return
        return data
    
    def calendar_D360_to_standard (dataset):
    
        # read start and end time and date from calendar no leap
        start_date = dataset.time.values[0].strftime("%Y-%m-%d %H:%M:%S")
        end_date = dataset.time.values[-1].strftime("%Y-%m-%d %H:%M:%S")
    
        # Februrary 29 for non leap years or February 30th 
        indices_30_feb = np.where((dataset['time'].dt.month == 2) & (dataset['time'].dt.day == 30))[0]
        indices_29_feb_no_leap = np.where((dataset['time'].dt.year % 4 != 0) & (dataset['time'].dt.month == 2) & (dataset['time'].dt.day == 29))[0]
    
        # Get indices that are to be kept (not removed)
        indices_to_remove = np.concatenate((indices_30_feb, indices_29_feb_no_leap))
        indices_to_keep = np.setdiff1d(np.arange(len(time)), indices_to_remove)
        
        # Drop the identified indices from the dataset using isel
        dataset = dataset.isel(time=indices_to_keep)
        
        # standard time
        standard_date = xr.cftime_range(start=start_date, end=end_date, freq='D', calendar='standard')
        
        # create standard time without 31 days of the month to be compatibale with calendar 360D
        indices_to_remove = np.where(standard_date.day != 31)[0]
    
        # Drop the identified indices from the time range
        standard_date_modified = standard_date[indices_to_remove]
        
        # set coordinate
        dataset = dataset.assign_coords(time=standard_date_modified)
        
        # Extend the time dimension to include the standard calendar dates
        dataset = dataset.reindex(time=standard_date, method='pad')
        
        # return
        return dataset
    
    def agg_hourl_to_daily (ds, offset=0, stat='mean'):
        
        import copy
        ds = copy.deepcopy(ds)
        
        # Roll the time based on the hour difference for more accurate alignment
        if offset != 0:
            ds['time'] = ds['time'].roll(time=-offset)
            if offset > 0:
                ds = ds.isel(time=slice(None, -offset))
            elif offset < 0:
                ds = ds.isel(time=slice(-offset, None))
            
        # Create the xarray dataframe with daily time
        if stat == 'max':
            ds_daily = ds.resample(time='D').max()
        elif stat == 'min':
            ds_daily = ds.resample(time='D').min()
        elif stat == 'mean':
            ds_daily = ds.resample(time='D').mean()
        elif stat == 'sum':
            ds_daily = ds.resample(time='D').sum()
        else:
            sys.exit('input stat should be max, min, mean, or sum')
        
        return ds_daily
    
    
    def agg_daily_to_hourly (ds_daily, ds_hourly_pattern):
        
        # to be populated
        
        A = None
        
        return A
