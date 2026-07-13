from pathlib import Path

pytest_plugins = (
    'celery.contrib.pytest',
    'climatoology.test.fixtures.base',
    'climatoology.test.fixtures.alembic',
    'climatoology.test.fixtures.aoi',
    'climatoology.test.fixtures.artifact',
    'climatoology.test.fixtures.computation',
    'climatoology.test.fixtures.database',
    'climatoology.test.fixtures.plugin',
    'climatoology.test.fixtures.plugin_info',
    'climatoology.test.fixtures.utility',
    'climatoology.test.fixtures.utility.naturalness',
)
TEST_RESOURCES_DIR = Path(__file__).parent / 'resources'
