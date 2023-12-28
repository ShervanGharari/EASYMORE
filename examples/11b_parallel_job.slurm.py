#!/bin/bash
#SBATCH --account=rpp-kshook
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=1G
#SBATCH --time=00:10:00           # time (DD-HH:MM)
#SBATCH --job-name=easymore_test_parallel
#SBATCH --error=easymore_test_parallel_error

# remove existing modules
cd
module reset
module purge
deactivate

# load needed modules
module load StdEnv/2020 gcc/9.3.0 openmpi/4.0.3
module load gdal/3.5.1 libspatialindex/1.8.5
module load python/3.8.10 scipy-stack/2022a mpi4py/3.0.3

# create the virtual env and install easymore from git repo
virtualenv $SLURM_TMPDIR/easymore-env
source $SLURM_TMPDIR/easymore-env/bin/activate
pip install --no-index --upgrade pip
# pip install easymore # from pypi
# pip install --no-index easymore # install from CC wheelhouse, similar to above this can be installed locally or from pypi
pip install git+https://github.com/ShervanGharari/EASYMORE.git@develop_2.0.0

# check if the code runs smoothly given the example on the easymore github repo
python 11b_parallel_job.py # compare the result with repository