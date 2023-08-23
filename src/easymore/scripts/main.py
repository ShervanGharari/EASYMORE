"""
easymore's main CLI script
"""

import pkgutil
import subprocess
import tempfile
import json

import click

from ._cli_options import cli_options
from ._cli_args import cli_args
from ._conf_options import conf_options
from ._conf_args import conf_args
from ._submit_options import submit_options
from ._submit_args import submit_args

from easymore import Easymore

DEFAULT_SCRIPT = "assets/default.slurm"


# `main` options and arguments
# `cli` options and arguments
cli_o = [click.option(*k, **v) for k, v in cli_options.items()]
cli_a = [click.argument(*k, **v) for k, v in cli_args.items()]
# `conf` options and arguments
conf_o = [click.option(*k, **v) for k, v in conf_options.items()]
conf_a = [click.argument(*k, **v) for k, v in conf_args.items()]
# `submit` options and arguments
submit_o = [click.options(*k, **v) for k, v in submit_options.items()]
submit_a = [click.argument(*k, **v) for k, v in submit_args.items()]

# Epilog help message
epilog = "For bug reports, questions, and discussions open an issue at" + \
    " https://github.com/ShervanGharari/EASYMORE.git"
width = 74


def add_decorator(options):
    """repeats decorators
    """
    def _add_decorator(func):
        for option in reversed(options):
            func = option(func)
        return func
    return _add_decorator


@click.group(context_settings={'max_content_width': width},
             epilog=epilog)
@click.version_option()
def main():
    """
    EASYMORE is a collection of functions that allows extraction of the
    data from a NetCDF file for a given shapefile such as a basin,
    catchment, points or lines. It can map gridded data or model output to
    any given shapefile and provide area average for a target variable.
    """


@main.command('cli', no_args_is_help=True)
@add_decorator(cli_o)
def from_cli(**kwargs):
    """
    Run Easymore using CLI
    """
    # two job submission related options
    job_var = 'submit_job'
    job_conf = 'submit_job_conf'
    # checking if submitting a job to HPC schedulers
    if kwargs[job_var]:
        # creating parameter dictionary for Easymore
        esmr_kwargs = {k: v for k, v in kwargs.items() if k not in
                       (job_var, job_conf)}
        # checking if a job submission configuration file is given
        if kwargs[job_conf] != 'default':
            submission_conf = kwargs[job_conf]
        else:
            submission_conf = DEFAULT_SCRIPT

        # [FIXME]: make `submit_hpc_job` more flexible
        # since `submit_hpc_job` only accepts a JSON file, create a
        # temporary one out of the dictionary and pass it to the function
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
            json.dump(esmr_kwargs, f, indent=4)

        submit_hpc_job(f.name, submission_conf)
    # if no job submission
    else:
        cli_exp = Easymore.from_dict(kwargs)
        cli_exp.nc_remapper()


@main.command('conf', no_args_is_help=True)
@add_decorator(conf_o)
@add_decorator(conf_a)
def from_conf(json, **kwargs):
    """
    Run Easymore using a JSON configuration file
    """
    # if job submission
    job_var = 'submit_job'
    job_conf = 'submit_job_conf'
    if kwargs[job_var]:
        if kwargs[job_conf] != 'default':
            submission_conf = kwargs[job_conf]
        else:
            submission_conf = DEFAULT_SCRIPT
        submit_hpc_job(json, submission_conf)
    # if no job submission
    else:
        json_exp = Easymore.from_json_file(json)
        json_exp.nc_remapper()


def submit_hpc_job(
    json: str,
    job_conf: str,
):
    """
    Submit easymore experiment to SLURM scheduler
    """
    # Read the file content using resources
    job_str = pkgutil.get_data(__name__, job_conf).decode()

    esmr_text = f'easymore conf {json}'
    job_str = job_str + '\n' + esmr_text

    # encode `job_script` string into byte-like object
    job_byte = job_str.encode()

    # export new script file
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
        f.write(job_byte)

    subprocess.run(["sbatch", f.name])


def _kwargs_to_text(**kwargs):
    text = ""
    for key, value in kwargs.items():
        key = '--' + key.replace('_', '-')
        text += f"{key}={value}\n"
    return text
