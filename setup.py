from setuptools import find_packages, setup

setup(
    name='easymore',
    version='0.0.0',
    license='GPLv3',
    author=('Shervan Gharari', 'Wouter Knoben'),
    install_requires=[
        'numpy',
        'geopandas >= 0.8.1',
        'xarray',
        'pandas',
        'shapely',
        'netCDF4',
        'datetime',
        'simpledbf',
        'pyshp',
        'pysheds',
        'osgeo',
        'geovoronoi',
        'json5',
        'rasterio',
    ],
    author_email='sh.gharari@gmail.com',
    description=(
        'Extract catchment data from netcdf file based on a catchment shapefile'
    ),
    long_description=(
        'Extract catchment data from netcdf file based on a catchment shapefile'
    ),
    packages=find_packages(),
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GPLv3',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    scripts=['easymore/easymore.py']
)
