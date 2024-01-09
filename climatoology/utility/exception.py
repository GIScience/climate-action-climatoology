class PlatformUtilityException(Exception):
    """A requested utility exited exceptionally."""
    pass


class InfoNotReceivedException(Exception):
    """A plugin did not respond in time."""
    pass


class ClimatoologyVersionMismatchException(Exception):
    """The plugins' library version does not match the required minimum version."""
    pass
