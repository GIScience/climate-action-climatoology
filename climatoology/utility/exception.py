from climatoology.base.logging import get_climatoology_logger

log = get_climatoology_logger(__name__)


class PlatformUtilityError(Exception):
    """A requested utility exited exceptionally."""

    pass
