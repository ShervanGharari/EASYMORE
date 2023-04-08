# Example of installing EASYMORE in virtual environment

## Mac

First we need to have GDAL source files. On mac this can be done by

```brew install gdal```

If you already have installed gdal you can create a virtual env. Here we create the virtual environment with Python3.8

```
conda create --name easymore-env python=3.8 --no-default-packages
```
After the environment is created we can activate this and check its information:
```
conda activate easymore-env
conda info
```
Then install gdal from inside the virtual env
```
conda install -c conda-forge gdal
```
We can then install packages needed for testing the examples:
```
python3.8 -m pip install --upgrade pip
python3.8 -m pip install jupyter
python3.8 -m pip install ipykernel
python3.8 -m ipykernel install --name=easymore-env # Add the new virtual environment to Jupyter
jupyter kernelspec list # list existing Jupyter virtual environments
```
Next is to install easymore; from PyPI we can easily do:
```
python3.8 -m pip install easymore
```
If installation from the latest code is desired we can do:
```
git clone https://github.com/ShervanGharari/EASYMORE.git
cd EASYMORE
python3.8 -m pip install .
```
Next is to navigate to the example and see if we can sucessfully simulated them; we can use jyputer notebook directly by opening the examples:
```
cd examples
jupyter notebook
```
Or convert the jupyter notebook into python and use python to run the scripts:
```
jupyter nbconvert *.ipynb --to python # converts all the files to .py
python3.8 01_ERA5_Regular_Lat_Lon.py
```