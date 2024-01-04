"""
Arguments to run the CONF command of the easymore program
"""

import click

conf_args = {
    ('JSON',): {
        'required': False,
        'type': click.Path(exists=True,
                           file_okay=True,
                           dir_okay=False,
                           readable=True),
        'nargs': 1,
        'metavar': '[CONF]',
    }
}
