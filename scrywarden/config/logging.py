from logging.config import dictConfig

from scrywarden.config import parsers
from scrywarden.config.base import Config

LOGGING_PARSER = parsers.Options({
    'version': parsers.Integer(default=1),
    'formatters': parsers.Dict(parsers.Options({
        'format': parsers.String(),
        'datefmt': parsers.String(),
    })),
    'handlers': parsers.Dict(parsers.Options({
        'class': parsers.String(required=True),
        'level': parsers.String(),
        'formatter': parsers.String(),
        'filters': parsers.List(parsers.String()),
    })),
    'loggers': parsers.Dict(parsers.Options({
        'level': parsers.String(),
        'propagate': parsers.Boolean(),
        'filters': parsers.List(parsers.String()),
        'handlers': parsers.List(parsers.String()),
    })),
    'root': parsers.Options({
        'level': parsers.String(),
        'filters': parsers.List(parsers.String()),
        'handlers': parsers.List(parsers.String()),
    }),
    'disable_existing_loggers': parsers.Boolean(),
}, default={
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console'],
    },
    'disable_existing_loggers': False,
})


def configure_logging(config: Config):
    config = config.parse(LOGGING_PARSER)
    dictConfig(config.value)
