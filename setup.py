from setuptools import find_packages, setup
# import subprocess
#from sys import platform
#import re

# def check_gdal_version_string(string):
#     '''Returns boolean True if string is in format x.x.x'''
#     matches = re.match(r"[0-9]+\.[0-9]+\.[0-9]+$", string)
#     return matches

# def get_installed_gdal_version():
#     '''Returns string of GDAL version in format of ==x.x.x.* is gdal exists, ohterwise empty or raise error'''
#     try:
#         # get the gdal version
#         if ('linux' in str(platform).lower()) or ('darwin' in str(platform).lower()):
#             gdal_version = subprocess.run(["gdal-config","--version"], stdout=subprocess.PIPE).stdout.decode()
#         elif ('win' in str(platform).lower()):
#             gdal_version = subprocess.run(["gdalinfo","--version"], stdout=subprocess.PIPE).stdout.decode()
#             gdal_version = gdal_version.replace('GDAL','').split(',')[0].strip()
#         # Check we got the version string in the correct format
#         if 'gdal_version' in locals():
#             if check_gdal_version_string(gdal_version):
#                 gdal_version = gdal_version.replace('\n', '')
#                 print("GDAL version is detected as "+gdal_version)
#                 gdal_version = "=="+gdal_version+".*"
#             else:
#                 print("GDAL version not returned in expected format."+\
#                 "Should be x.x.x. Was {} and will be reset to empty string".format(gdal_version))
#                 gdal_version = ''
#         else:
#             print("GDAL version not returned and will be reset "+\
#                   "to empty string for pip to decide/install gdal")
#             gdal_version = ''
#         return gdal_version
#     except FileNotFoundError as e:
#         raise(""" ERROR: Could not find the system install of GDAL.
#                   Please install it via your package manage of choice.
#                 """
#             )

# read the contents of your README file for distribution on PyPI
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='easymore',
    version='1.0.0',
    license='GPLv3',
    author=('Shervan Gharari'),
    author_email = 'sh.gharari@gmail.com',
    url = 'https://github.com/ShervanGharari/EASYMORE',
    description=(
        'geo-spatial processing of the input data for environmental and hydrological modeling'
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',
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
        'cftime',
        'geopandas',
        'shapely',
        'geovoronoi',
        'json5',
        'matplotlib',
        'rtree'
    ], # 'gdal'+get_installed_gdal_version(),  'pyshp',
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
