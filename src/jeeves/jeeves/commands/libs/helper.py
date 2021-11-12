import click
import subprocess


class Helper:

    @classmethod
    def execute(cls, cmd, cwd):
        try:
            click.echo(' '.join(cmd))
            process = subprocess.Popen(cmd,
                                       cwd=cwd,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)

            output, error = process.communicate()

            ignore_errors = [
                'INVALID_ARGUMENT: Must specify a change to PYPI_DEPENDENCIES',
                'INVALID_ARGUMENT: Must specify a change to ENV_VARIABLES'
                ]

            if process.returncode != 0:
                result = list(filter(lambda f: f in error.decode('utf-8'), ignore_errors))
                if (len(result) == 0):
                    click.echo(error)
                    exit(process.returncode)

        except Exception as e:
            click.echo(f'There was an error executing cmd: {cmd} in {cwd}', e)
            exit(1)
