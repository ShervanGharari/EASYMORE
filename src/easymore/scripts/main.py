"""
easymore's main CLI script
"""

import pkgutil
import subprocess

import click

from ._cli_options import cli_options
from ._cli_args import cli_args
from ._conf_options import conf_options
from ._conf_args import conf_args
from ._submit_options import submit_options
from ._submit_args import submit_args

from easymore import Easymore

DEFAULT_SCRIPT = "assets/default.slurm"


def add_options(options):
    """Adding options to @click.command decorated functions
    """
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func
    return _add_options


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
    "https://github.com/ShervanGharari/EASYMORE.git"
width = 74


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


@main.command('cli')
@add_options(cli_o)
def from_cli(**kwargs):
    """
    Run Easymore using CLI
    """
    job_var = 'submit_job'
    job_conf = 'submit_job_conf'
    if kwargs[job_var]:
        esmr_kwargs = {k: v for k, v in kwargs.items() if k not in
                       (job_var, job_conf)}
        try:
            submission_conf = kwargs[job_conf]
        except KeyError:
            submission_conf = DEFAULT_SCRIPT
        submit_hpc_job(esmr_kwargs, submission_conf)
    # if no job submission
    else:
        cli_exp = Easymore.from_dict(kwargs)
        cli_exp.nc_remapper()


@main.command('conf', no_args_is_help=True)
@add_options(conf_o)
@add_options(conf_a)
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
    job_script = pkgutil.get_data(__name__, job_conf).decode()

    esmr_text = f'easymore conf {json}'
    job_script = job_script + '\n' + esmr_text

    # export new script file
    with open('temp_script.slurm', 'w') as f:
        f.write(job_script)

    subprocess.run(["sbatch", "temp_script.slurm"])


def _kwargs_to_text(**kwargs):
    text = ""
    for key, value in kwargs.items():
        key = '--' + key.replace('_', '-')
        text += f"{key}={value}\n"
    return text
