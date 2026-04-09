from importlib import resources
from importlib.metadata import version

from semver import Version

__version__: Version = Version.parse(version('climatoology'))

RESOURCES_DIR = resources.files() / 'resources'
