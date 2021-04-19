from setuptools import find_packages, setup

setup(
    name='easymore',
    version='0.0.0',
    license='GPLv3',
    author=('Shervan Gharari', 'Wouter Knoben'),
    author_email = 'sh.gharari@gmail.com',      # Type in your E-Mail
    url = 'https://github.com/ShervanGharari/EASYMORE',   # Provide either the link to your github or to your website
    download_url = 'https://github.com/user/reponame/archive/v_01.tar.gz',    # I explain this later on
    keywords = ['remapping', 'NetCDF',
        'shapefile','geotif',
        'geo-spatial processing',
        'environmental modeling'],
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
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GPLv3',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    scripts=['easymore/easymore.py']
)
