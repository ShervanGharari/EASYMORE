"""
Options to run the CONF command of the easymore program
"""

import click

conf_options = {
    ('--submit-job', '-j'): {
        'type': click.BOOL,
        'required': False,
        'is_flag': True,
        'allow_from_autoenv': True,
        'help': 'Submit job to HPC scheduler',
    },
    ('--job-conf', '-jc'): {
        'type': click.STRING,
        'required': False,
        'default': 'default',
        'show_default': True,
        'is_flag': False,
        'allow_from_autoenv': True,
        'help': 'Job submission script file',
        'show_choices': False,
    },
    ('--dependency', '-p'): {
        'type': click.STRING,
        'required': False,
        'default': None,
        'multiple': True,
        'is_flag': False,
        'allow_from_autoenv': True,
        'help': 'Make SLURM job submission dependant on successfully'
        ' finishing another on given their submission ID',
        'show_choices': False,
    }
}
