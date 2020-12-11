"""CLI commands of the application."""

import click
from click import Context

import scrywarden.database as db
from scrywarden.curator import Curator
from scrywarden.investigator.base import parse_investigators
from scrywarden.profile.base import sync_profiles
from scrywarden.profile.config import parse_profiles
from scrywarden.pipline.base import Pipeline
from scrywarden.config import parse_config, Config
from scrywarden.config.logging import configure_logging
from scrywarden.shipper import parse_shippers
from scrywarden.transport.config import parse_transports


@click.group()
@click.option(
    '-c', '--config', default='scrywarden.yml',
    help="Path to the config file to use.", show_default=True,
)
@click.pass_context
def main(ctx: Context, **kwargs):
    """Detects anomalies in datasets using behavioral modeling."""
    ctx.ensure_object(dict)
    ctx.obj['config_file'] = kwargs['config']


def setup(ctx: Context):
    config = parse_config(ctx.obj['config_file'])
    engine = db.parse_engine(config.get('database', {}))
    configure_logging(config.get('logging'))
    db.migrate(engine)
    ctx.obj['config'] = config
    ctx.obj['session_factory'] = db.create_session_factory(engine)


@main.command()
@click.pass_context
def collect(ctx: Context):
    """Collect messages to populate behavioral profiles."""
    setup(ctx)
    config: Config = ctx.obj['config']
    transports = tuple(parse_transports(config.get('transports')).values())
    profiles = []
    for profile_objects in parse_profiles(config['profiles']).values():
        profiles.append(profile_objects['profile'])
    profiles = tuple(profiles)
    with db.managed_session(
        ctx.obj['session_factory'], expire_on_commit=False,
    ) as session:
        sync_profiles(session, profiles)
    pipeline = Pipeline(transports, profiles, ctx.obj['session_factory'])
    pipeline.configure(config.get('pipeline'))
    pipeline.start()


@main.command()
@click.pass_context
def investigate(ctx: Context):
    """Investigate current anomalies to find malicious activity."""
    setup(ctx)
    config: Config = ctx.obj['config']
    investigators = parse_investigators(config['profiles'])
    shippers = parse_shippers(config['shippers']).values()
    curator = Curator(
        investigators, shippers, session_factory=ctx.obj['session_factory'],
    )
    curator.start()


if __name__ == '__main__':
    main()
