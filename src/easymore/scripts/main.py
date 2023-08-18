"""
easymore's CLI
"""

import click

'''from easymore import Easymore'''

options = {
    ('--cache', '-c'): {
        'type': click.Path(exists=False, file_okay=False),
        'required': False,
        'default': '',
        'is_flag': False,
        'allow_from_autoenv': True,
        'help': 'Temporary directory',
        'show_choices': False,
    },
    ('--shapefile', '-a'): {
        'type': click.Path(exists=True, file_okay=True, dir_okay=False),
        'required': False,
        'default': '',
        'is_flag': False,
        'allow_from_autoenv': True,
        'help': 'Path to the ESRI Shapefile',
        'show_choices': False,
    },
    ('--id', '-i'): {
        'type': click.STRING,
        'required': False,
        'default': '',
        'is_flag': False,
        'allow_from_autoenv': True,
        'help': 'Column name corresponding to the IDs of the ESRI'
        ' Shapefile features',
        'show_choices': False,
    },
}

args = {
    ('conf'): {
        'nargs': 1,
        'type': click.Path(exists=True),
    },
}


def add_options(options):
    """Adding options to @click.command decorated functions
    """
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func
    return _add_options


o = [click.option(*k, **v) for k, v in options.items()]
a = [click.option(*k, **v) for k, v in args.items()]


@click.command()
@add_options(o)
@click.version_option()
def cli():
    click.echo("CLI Hit")


if __name__ == '__main__':
    cli()
