from setuptools import find_packages, setup
import subprocess

def get_installed_gdal_version():
    try:
        version = subprocess.run(["gdal-config","--version"], stdout=subprocess.PIPE).stdout.decode()
        version = version.replace('\n', '')
        version = "=="+version+".*"
        return version
    except FileNotFoundError as e:
        raise(""" ERROR: Could not find the system install of GDAL.
                  Please install it via your package manage of choice.
                """
            )

setup(
    name='easymore',
    version='0.0.3',
    license='GPLv3',
    author=('Shervan Gharari', 'Wouter Knoben'),
    author_email = 'sh.gharari@gmail.com',
    url = 'https://github.com/ShervanGharari/EASYMORE',
    keywords = ['remapping', 'NetCDF',
        'shapefile','geotif',
        'geo-spatial processing',
        'environmental modeling'],
    install_requires=[
        'numpy',
        'xarray',
        'pandas',
        'netCDF4',
        'datetime',
    ],
    extras_require={
    "complete":['geopandas >= 0.8.1',
    'shapely',
    'pyshp',
    'pysheds',
    'gdal'+get_installed_gdal_version(),
    'geovoronoi',
    'json5',
    'rasterio',
    'rtree',]
    },
    description=(
        'geo-spatial processing of the input data for environmental and hydrological modeling'
    ),
    long_description=(
        'Extract data from netcdf file from any shape to any shape (such as catchments), geo-spatial processes such as creating subbains and river network topology, zonl statistics'
    ),
    packages=find_packages(),
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    scripts=['easymore/easymore.py']
)