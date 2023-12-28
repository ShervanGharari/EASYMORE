python3.9 -m pip install virtualenv
python3.9 -m virtualenv easymore-env
source easymore-env/bin/activate
python3.9 -m pip install --upgrade pip
#python3.9 -m pip install easymore # from pypyt
#python3.9 -m pip install git+https://github.com/ShervanGharari/EASYMORE.git@main # directly from a github branch
python3.9 -m pip install ../. # local or path/to/easymore
python3.9 -m pip install jupyter
python3.9 -m pip install ipykernel
python3.9 -m ipykernel install --name=easymore-env # Add the new virtual environment to Jupyter
jupyter kernelspec list # list existing Jupyter virtual environments
#jupyter notebook # launch jupyter notebook; it should be done at example location