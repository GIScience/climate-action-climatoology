# from importlib.metadata import version

from semver import Version

# TODO: change this back when releasing v7
# version('climatoology')
__version__: Version = Version.parse('7.0.0-rc1')
