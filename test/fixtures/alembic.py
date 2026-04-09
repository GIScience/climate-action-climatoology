from datetime import datetime

import pytest
import sqlalchemy
from pytest_alembic import Config

from climatoology.base.computation import ComputationState
from test.fixtures.database import connection_to_string


@pytest.fixture
def alembic_engine(db_fixture_basic, set_basic_envs):
    connection_str = connection_to_string(db_fixture_basic)
    return sqlalchemy.create_engine(connection_str)


@pytest.fixture
def alembic_config(
    general_uuid,
    default_plugin_info_final,
    default_plugin_info_final_de,
    default_computation_info,
    default_computation_info_de,
    default_artifact_enriched,
    default_artifact_enriched_de,
) -> Config:
    return Config(
        config_options={'script_location': 'climatoology/store/database/migration'},
        at_revision_data={
            '0c77b1f5b970': [
                {
                    '__tablename__': 'celery_taskmeta',
                    'id': 1,
                    'task_id': str(general_uuid),
                    'date_done': datetime.now(),
                }
            ],
            '3d4313578291': [
                {
                    '__tablename__': 'info',
                    'plugin_id': default_plugin_info_final.id,
                    'name': default_plugin_info_final.name,
                    'version': str(default_plugin_info_final.version),
                    'concerns': ['CLIMATE_ACTION__GHG_EMISSION'],
                    'purpose': default_plugin_info_final.purpose,
                    'methodology': default_plugin_info_final.methodology,
                    'assets': default_plugin_info_final.assets.model_dump(mode='json'),
                    'operator_schema': default_plugin_info_final.operator_schema,
                    'library_version': str(default_plugin_info_final.library_version),
                },
                {'__tablename__': 'pluginauthor', 'name': 'Waldemar'},
                {
                    '__tablename__': 'author_info_link_table',
                    'info_id': default_plugin_info_final.id,
                    'author_id': 'Waldemar',
                },
            ],
            '49cccfd144a8': [
                {
                    '__tablename__': 'ca-base.computation',
                    'correlation_uuid': str(general_uuid),
                    'timestamp': datetime.now(),
                    'params': default_computation_info.params,
                    'aoi_geom': default_computation_info.aoi.geometry.wkt,
                    'aoi_name': default_computation_info.aoi.properties.name,
                    'aoi_id': default_computation_info.aoi.properties.id,
                    'plugin_id': default_computation_info.plugin_info.id,
                    'plugin_version': str(default_computation_info.plugin_info.version),
                    'status': ComputationState.PENDING,
                    'artifact_errors': default_computation_info.artifact_errors,
                },
                {
                    '__tablename__': 'ca-base.artifact',
                    'correlation_uuid': str(general_uuid),
                    'name': default_artifact_enriched.name,
                    'modality': default_artifact_enriched.modality.value,
                    'primary': default_artifact_enriched.primary,
                    'summary': default_artifact_enriched.summary,
                    'store_id': default_artifact_enriched.filename,
                    'file_path': default_artifact_enriched.filename,
                },
            ],
            '8b52ceba3457': [
                {
                    '__tablename__': 'ca-base.computation_lookup',
                    'user_correlation_uuid': str(general_uuid),
                    'request_ts': datetime.now(),
                    'computation_id': str(general_uuid),
                }
            ],
            'bf7b34435593': [
                {
                    '__tablename__': 'ca_base.plugin_info',
                    'id': default_plugin_info_final_de.id,
                    'version': str(default_plugin_info_final_de.version),
                    'language': default_plugin_info_final_de.language,
                    'name': default_plugin_info_final_de.name,
                    'repository': str(default_plugin_info_final_de.repository),
                    'state': str(default_plugin_info_final_de.state).upper(),
                    'concerns': ['CLIMATE_ACTION__GHG_EMISSION'],
                    'teaser': default_plugin_info_final_de.teaser,
                    'purpose': default_plugin_info_final_de.purpose,
                    'methodology': default_plugin_info_final_de.methodology,
                    'demo_config': default_plugin_info_final_de.demo_config.model_dump(mode='json'),
                    'assets': default_plugin_info_final_de.assets.model_dump(mode='json'),
                    'operator_schema': default_plugin_info_final_de.operator_schema,
                    'library_version': str(default_plugin_info_final_de.library_version),
                    'latest': True,
                },
                {
                    '__tablename__': 'ca_base.computation',
                    'correlation_uuid': str(default_computation_info_de.correlation_uuid),
                    'language': default_computation_info_de.language,
                    'valid_until': datetime.now(),
                    'params': default_computation_info_de.params,
                    'requested_params': default_computation_info_de.requested_params,
                    'aoi_geom': default_computation_info_de.aoi.geometry.wkt,
                    'plugin_key': f'{default_computation_info_de.plugin_info.id}-{default_computation_info_de.plugin_info.version}-{default_computation_info_de.plugin_info.language}',
                    'artifact_errors': default_computation_info_de.artifact_errors,
                },
                {
                    '__tablename__': 'ca_base.computation_lookup',
                    'user_correlation_uuid': str(default_computation_info_de.correlation_uuid),
                    'request_ts': datetime.now(),
                    'aoi_name': 'AOI Name',
                    'aoi_id': 'aoi_id',
                    'is_demo': False,
                    'computation_id': str(default_computation_info_de.correlation_uuid),
                },
                {
                    '__tablename__': 'ca_base.artifact',
                    'rank': 0,
                    'correlation_uuid': str(default_computation_info_de.correlation_uuid),
                    'name': default_artifact_enriched_de.name,
                    'tags': default_artifact_enriched_de.tags,
                    'modality': default_artifact_enriched_de.modality.value,
                    'primary': default_artifact_enriched_de.primary,
                    'summary': default_artifact_enriched_de.summary,
                    'attachments': default_artifact_enriched_de.attachments.model_dump(mode='json'),
                    'filename': default_artifact_enriched_de.filename,
                },
            ],
        },
    )
