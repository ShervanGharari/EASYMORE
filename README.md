[![DOI](https://zenodo.org/badge/316842913.svg)](https://zenodo.org/badge/latestdoi/316842913)

# EASYMORE; EArth SYstem MOdeling REmapper:

This package allows you to extract and aggregate the relevant values from a
cfconventions compliant netcdf files given shapefiles.

EASYMORE is a collection of functions that allows extraction of the data from a NetCDF file for a given shapefile such as a basin, catchment, points or lines. It can map gridded data or model output to any given shapefile and provide area average for a target variable.

EASYMORE is very efficient as it uses pandas `groupby` functionality. Remapping of the entire north American domain from ERA5 with resolution of 0.25 degree to 500,000 subbasins of MERIT-Hydro watershed for 7 variables in 1.2 seconds for one time step (the time varying from device to device and depending on the source netCDF files sizes and their temporal aggregation).

In addition, EASYMORE can perform geospatial processes that are often needed for setting up a hydroligcal or land model. These steps are zonal statistics of raster given a shapefile, zonal statistics of two shapefiles, creation of subbasins and river network topology given hydrologically conditioned DEM such as MERIT-Hydro.

## The code can be used for the following purposes:

1. Remapping the relevant forcing variables, such as precipitation or temperature and other variables for the effortless model set up. This transfer can be from Thiessen polygon or gridded data, for example, to computational units, hydrological model for example.
2. Remapping the output of a hydrological or land surface model to force another model, such as providing the gridded model output in sub-basin for routing.
3. Extraction of single or multiple points from the gridded or irregular data for comparison with gauges data, for example.
4. Interpolation to caorser or finer resolutions with full controllability in creating the interpolation rules.

## How to install:

### From PyPI:

`pip install easymore`

### From local repo:

clone the code on your perosnal computer or home on HPC by

```
git clone https://github.com/ShervanGharari/EASYMORE.git
cd EASYMORE
pip install -r requirements.txt
pip install .
```

## Flexibilities:

1. EASYMORE allow for commbination of the remapping of NetCDF on local computer or remote high performance computer. For example, the the GIS steps of creating remapping file can be done locally on a sample file that contains few time step of the data (but all the domain). EASYMORE can then be directed to remapping file on the HPC and will skip all the needed GIS steps and directly start remapping process of bulk of the data. For installing without gis packages you can simply do:

### From local repo:

```
git clone https://github.com/ShervanGharari/EASYMORE.git
cd EASYMORE
pip install -r requirements.txt
pip install . --no-deps -r requirements_basic.txt
```

## Examples:

1. [Remapping a regular lat/lon gridded data or model output to irregular shapes.](./examples/Chapter1_E1.ipynb)
2. [Remapping a rotate lat/lon gridded data or model output to irregular shapes.](./examples/Chapter1_E2.ipynb)
3. [Remapping an irregular shapefile data, such as Thiessen polygon for example, to irregular shapes.](./examples/Chapter1_E3.ipynb)
4. [Extract the data for points (such as location of stations, cities, etc) from the grided or irregular shapefiles; temprature example](./examples/Chapter1_E4.ipynb)
5. [Resampling of regular, rotated or irregular data and model output to any resolution density of the points](./examples/Chapter1_E5.ipynb)
6. [Manipulation of remapped variables such as lapse rate to temperature based on elevation difference (can be applied to slope and aspect, etc)](./examples/Chapter1_E6.ipynb)
7. [Creation of subbasins for hydrologically conditioned MERIT-Hydro DEM and perform spatial processes for finding elevation and land cover characteistics of all the subbasins](./examples/Chapter2_E1.ipynb)
8. [Creation of Thiessen polygons, sampling a from a geotif based on point shapefile](./examples/Chapter2_E2.ipynb)

The two figures show remapping of the gridded temperature from ERA5 data set to subbasin of South Saskatchewan River at Medicine Hat.

### Original gridded temperature field:

![](https://github.com/ShervanGharari/EASYMORE/blob/main/fig/Gird.png)

### Remapped temperature field to the subbasins:

![](https://github.com/ShervanGharari/EASYMORE/blob/main/fig/Remapped.png)

## Publication that have used EASYMORE so far:

Authors

Knoben, W. J. M., Clark, M. P., Bales, J., Bennett, A., Gharari, S., Marsh, C. B., Nijssen, B., Pietroniro, A., Spiteri, R. J., Tarboton, D. G., Wood, A. W.: Community Workflows to Advance Reproducibility in Hydrologic Modeling: Separating model-agnostic and model-specific configuration steps in applications of large-domain hydrologic models, Earth and Space Science Open Archive, 42, https://doi.org/10.1002/essoar.10509195.1, 2021

Gharari, S., Vanderkelen, I., Tefs, A., Mizukami, N., Stadnyk, T. A., Lawrence, D., Clark, M. P.: A Flexible Multi-Scale Framework to Simulate Lakes and Reservoirs in Earth System Models, Earth and Space Science Open Archive, 24, https://doi.org/10.1002/essoar.10510902.1, 2022

Gharari, S., Clark, M. P., Mizukami, N., Knoben, W. J. M., Wong, J. S., and Pietroniro, A.: Flexible vector-based spatial configurations in land models, Hydrol. Earth Syst. Sci., 24, 5953–5971, https://doi.org/10.5194/hess-24-5953-2020, 2020

Sheikholeslami, R., Gharari, S., Papalexiou, S. M., Clark, M. P.: VISCOUS: A Variance-Based Sensitivity Analysis Using Copulas for Efficient Identification of Dominant Hydrological Processes, Water Resources Research, https://doi.org/10.1029/2020WR028435, 2021.
