from setuptools import find_packages, setup

# read the contents of your README file for distribution on PyPI
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='easymore',
    version='2.0.0',
    license='GPLv3',
    author=('Shervan Gharari'),
    author_email='sh.gharari@gmail.com',
    url='https://github.com/ShervanGharari/EASYMORE',
    description=(
        'geo-spatial processing of the input data for environmental and hydrological modeling'
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    keywords=[
        'remapping',
        'NetCDF',
        'shapefile',
        'geo-spatial processing',
        'environmental modeling'
    ],
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
        'rtree',
        'click'
    ],
    entry_points={
        'console_scripts': [
            'easymore = easymore.scripts.main:main',
        ],
    },
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
)
