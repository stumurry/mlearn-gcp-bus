import click
import logging
import warnings
from pyconfighelper import ConfigHelper
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud, Config

warnings.simplefilter("ignore")

log = logging.getLogger()
log.setLevel('DEBUG')


@click.group()
@pass_environment
def cli(ctx):
    pass


@cli.command()
@pass_environment
def create_keyring(ctx, env, location, keyring):
    pass


@cli.command()
@pass_environment
def create_key(ctx, env, location, keyring, keyname, rotation):
    pass


@cli.command()
@click.argument('env')
@click.argument('location')
@click.argument('keyring')
@click.argument('keyname')
@click.argument('plaintext')
@click.pass_context
def encode_secret(ctx, env, location, keyring, keyname, plaintext):
    """
    Encode a secret with the appropriate key from gcloud crypto keys

    jeeves crypto encode-secret ENV LOCATION KEYRING KEYNAME PLAINTEXT

    env = local, dev, txt, prd
    location = us-west3, us-west2, etc

    """
    helper = ConfigHelper(
        kms_project=gcloud.project(env),
        kms_location=location,
        kms_key_ring=keyring,
        kms_key_name=keyname,
        log_level=logging.ERROR)

    secret = helper.encode_secret(plaintext)

    return secret


@cli.command()
@click.argument('env')
@click.argument('location')
@click.argument('keyring')
@click.argument('keyname')
@click.argument('secret_name')
@click.argument('plaintext')
@click.pass_context
def create_secret(ctx, env, location, keyring, keyname, secret_name, plaintext):
    """
    jeeves crypto create-secret ENV LOCATION KEYRING KEYNAME SECRET_NAME PLAINTEXT

    env = local, dev, tst, prd

    location = 'us-west3', 'us-west2', etc

    Encodes the plaintext secret and writes it to gcloud secrets
    """
    encoded = ctx.invoke(encode_secret,
                         env=env,
                         location=location,
                         keyring=keyring,
                         keyname=keyname,
                         plaintext=plaintext)

    Config.add_secret(env, secret_name, encoded)


@cli.command()
@click.argument('env')
@click.argument('location')
@click.argument('secret_name')
@click.argument('secret_version')
@click.pass_context
def disable_secret_version(ctx, env, location, secret_name, secret_version):
    """
        jeeves disable_secret_version ENV LOCATION SECRET_NAME SECRET_VERSION
    """
    try:
        Config.disable_secret_version(env, secret_name, secret_version)
    except Exception as e:
        click.echo(e)
        exit(1)


@cli.command()
@click.argument('env')
@click.argument('location')
@click.argument('secret_name')
@click.pass_context
def list_secret_version(ctx, env, location, secret_name):
    """
        jeeves list_secret_version ENV LOCATION SECRET_NAME
        To do: remove location.  Not being used.
    """
    try:
        click.echo(f'Secret Version list for {secret_name}: ')
        for v in Config.list_secret_versions(env, secret_name):
            click.echo(v)
    except Exception as e:
        click.echo(e)
        exit(1)
