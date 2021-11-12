import click
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud
from jeeves.commands.libs.helper import Helper
from pathlib import PosixPath
import json
import os
from unittest.mock import patch


DAGS_LOCATION = 'us-west3'
VARIABLES_FILE = 'template_variables.json'
COMPOSER_VARIABLES_FILE = 'composer_variables.json'
REQUIREMENTS_FILE = 'requirements.txt'
AIRFLOW_VARIABLES_FILE = 'variables.json'


@click.group()
@pass_environment
def cli(ctx):
    "Deploy dag to Google Cloud Composer"
    pass


"""
    List of Commands
"""


@cli.command()
@click.argument('env')
@click.option('--test', '-t', is_flag=True, help="""Display by mocking environment""")
@click.argument('runtime_args', nargs=-1, required=False)
@pass_environment
def deploy_all(ctx, env, test, runtime_args):
    curr_path = os.path.dirname(__file__)
    vars_file_path = f'{curr_path}/{VARIABLES_FILE}'
    template_vars = _beam_vars(vars_file_path)
    dags = template_vars['dags']
    dag_templates = template_vars['dag_templates']
    clients = template_vars['clients']

    def deploy_dags_and_templates():
        # deploy the dags file
        for d in dags:
            _deploy_dag(ctx, env, d, True)

        for c in clients:
            for t in dag_templates:
                # Include any hard coded custom arguments
                template_args = (f'client={c}', f'env={env}')
                args = (template_args + runtime_args)
                _deploy_template(ctx, env, t, True, args)

    if test:
        with patch.object(Helper, 'execute', wraps=lambda cmd, cwd: click.echo(' '.join(cmd))):
            deploy_dags_and_templates()
    else:
        deploy_dags_and_templates()


@cli.command('deploy-dag', short_help='Deploy DAG to Cloud Composer')
@click.pass_context
@click.argument('env')
@click.argument('filename')
@click.option('--helper_files', '-h', is_flag=True, help="""Copy the contents of dags/templates folder,
    libs folder, and plugins folder into cloudstorage used in supporting the dags.
    All files here must remain independant of all other files in our repo.""")
def deploy_dag(ctx, env, filename, helper_files):
    """
    jeeves composer deploy-dag ENV TEMPLATE_FILENAME

    Deploy non-templated dag from dags directory.
    """
    _deploy_dag(ctx, env, filename, helper_files)


@cli.command('deploy-template', short_help='Deploy DAG template to Cloud Composer')
@click.pass_context
@click.argument('env')
@click.argument('filename')
@click.option('--helper_files', '-h', is_flag=True, help="""Copy the contents of dags/templates folder,
    libs folder, and plugins folder into cloudstorage used in supporting the dags.
    All files here must remain independant of all other files in our repo.""")
@click.argument('template_vars', nargs=-1, required=True)
def deploy_template(ctx, env, filename, helper_files, template_vars):
    _deploy_template(ctx, env, filename, helper_files, template_vars)


@cli.command('update-environment', short_help='Update Cloud Composer and its peripheral resources')
@click.pass_context
@click.argument('env')
@click.option('--composer_vars', '-c', is_flag=True, help="""Set Cloud Composer with the variables in the
    composer_variables.json file.""", default=True)
@click.option('--requirements', '-r', is_flag=True, help="""Specify if any dependencies need to be install in
    the environment use y to confirm""", default=True)
@click.option('--airflow_vars', '-a', is_flag=True, help="""Set Airflow enviroment with the airflow variables in the
    variables.json file.""", default=True)
def update_environment(ctx, env, composer_vars, requirements, airflow_vars):
    if airflow_vars:
        _update_airflow_vars(ctx, env)

    if composer_vars:
        _update_composer_vars(ctx, env)

    if requirements:
        _update_composer_requirements(ctx, env, False)


@cli.command('update-airflow-vars', short_help='Upload and Import the Airflow variables.json file')
@click.pass_context
@click.argument('env')
def update_airflow_vars(ctx, env):
    _update_airflow_vars(ctx, env)


@cli.command('update-composer-vars', short_help='Update Composer environment variables')
@click.pass_context
@click.argument('env')
def update_composer_vars(ctx, env):
    _update_composer_vars(ctx, env)


@cli.command('update-composer-requirements', short_help='Update Composer PyPI packages')
@click.pass_context
@click.argument('env')
@click.option('--run-async', '-a', is_flag=True, help='Run gcloud command with the --async flag', default=False)
def update_composer_requirements(ctx, env, run_async):
    _update_composer_requirements(ctx, env, run_async)


"""
    List of local helper methods
"""


def _update_airflow_vars(ctx, env):
    commands = []

    commands.append([
                    'gcloud',
                    'composer',
                    'environments',
                    'storage',
                    'dags',
                    'import',
                    '--source', '{}/{}'.format(_airflow_home_path(), AIRFLOW_VARIABLES_FILE),
                    '--environment', gcloud.project(env),
                    '--location', DAGS_LOCATION])

    commands.append([
                    'gcloud',
                    'composer',
                    'environments',
                    'run', gcloud.project(env),
                    '--location', DAGS_LOCATION,
                    'variables',
                    '--',
                    '-i',
                    f'/home/airflow/gcs/dags/{AIRFLOW_VARIABLES_FILE}'])

    for cmd in commands:
        Helper.execute(cmd=cmd, cwd=_dags_path())


def _update_composer_vars(ctx, env):
    composer_variables = get_composer_variables(env,  _dags_path().expanduser() / COMPOSER_VARIABLES_FILE)
    cmd = ['gcloud',
           'composer',
           'environments',
           'update', gcloud.project(env),
           '--update-env-variables', composer_variables,
           '--location', DAGS_LOCATION]

    Helper.execute(cmd=cmd, cwd=_dags_path())


def _update_composer_requirements(ctx, env, run_async):
    cmd = ['gcloud',
           'composer',
           'environments',
           'update', gcloud.project(env),
           '--update-pypi-packages-from-file', REQUIREMENTS_FILE,
           '--location', DAGS_LOCATION]

    if run_async:
        cmd.append('--async')

    Helper.execute(cmd=cmd, cwd=_dags_path())


def _deploy_dag(ctx, env, filename, helper_files):
    """
    jeeves composer deploy-dag ENV TEMPLATE_FILENAME

    Deploy non-templated dag from dags directory.
    """

    proj = gcloud.project(env)
    dags_path = _dags_path().expanduser()
    airflow_path = _airflow_home_path().expanduser()
    commands = []

    filename = f'{filename}.py' if '.py' not in filename else filename
    imports = [{'group': 'dags', 'source': f'{dags_path}/{filename}'}]

    if helper_files:
        imports.extend([
            {'group': 'dags', 'source': f'{dags_path}/libs'},
            {'group': 'dags', 'source': f'{dags_path}/templates'},
            {'group': 'dags', 'source': f'{dags_path}/extended_variables.json'},
            {'group': 'plugins', 'source': f'{airflow_path}/plugins/operators.py'},
            {'group': 'plugins', 'source': f'{airflow_path}/plugins/sensors.py'}])

    for i in imports:
        commands.append(
            ['gcloud',
             'composer',
             'environments',
             'storage',
             i['group'],
             'import',
             '--source', i['source'],
             '--environment', proj,
             '--location', DAGS_LOCATION])

    for cmd in commands:
        Helper.execute(cmd=cmd, cwd=_dags_path())


def _deploy_template(ctx, env, filename, helper_files, template_vars):
    """
    jeeves composer deploy-template ENV TEMPLATE_FILENAME TEMPLATE_VARS
    Deploy templated dag from templates directory.
    TEMPLATE_VARS Ex. client=bluesun foo=bar naughty=nice
    If ENV is 'local' then dag is built into dags directory but not pushed to composer.
    """
    placeholder_args = {}
    for item in template_vars:
        placeholder_args.update([item.split('=')])
    dag_filename = _build_dag_file(_dags_path(), filename, placeholder_args)
    if env == 'local':
        return dag_filename
    _deploy_dag(ctx, env, dag_filename, helper_files)
    with _dags_path().expanduser() / dag_filename as f:
        f.unlink()


def _build_dag_file(temp_dir, template_filename, placeholder_args):
    try:
        template = _templates_path().expanduser() / '{}.py'.format(template_filename)
        template = template.read_text()
        start = template.index('$dag_filename$:') + len('$dag_filename$:')
        end = template.index('.py')
        filename = template[start: end].strip() + '.py'

        for key, value in placeholder_args.items():
            template = template.replace('--{}--'.format(key), value)
            filename = filename.replace('--{}--'.format(key), value)
        new_dag = _dags_path().expanduser() / filename
        new_dag.write_text(template)
        return filename
    except ValueError as err:
        click.echo(f'{err}\nPlease include a filename in the template as a docstring.\
        \nEX: $dag_filename$: my_filename.py')
        exit(1)


def _dags_path():
    path = PosixPath('/workspace/airflow/dags/')
    return path


def _templates_path():
    path = PosixPath('/workspace/airflow/templates/')
    return path


def _airflow_home_path():
    path = PosixPath('/workspace/airflow/')
    return path


def get_composer_variables(env, composer_variables_file):
    with open(str(composer_variables_file)) as data_file:
        file_content = json.load(data_file)
        default_vars = file_content['default']
        env_vars = file_content[env]
        default_vars.update(env_vars)
        vars = list(default_vars.items())
        vars = ','.join([f'{n[0]}={n[1]}' for n in vars])
        return vars


def _beam_vars(vars_file):
    with open(vars_file) as d:
        vars = json.load(d)
        return vars
