class Error(Exception):
    """Base error class for the library."""


class ConfigError(Error):
    """Error with any configurations of the library."""


class ProfileError(Error):
    """Error occurred defining a profile."""
