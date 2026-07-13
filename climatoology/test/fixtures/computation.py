import uuid
from datetime import datetime
from typing import Generator

import pytest
from pydantic_extra_types.language_code import LanguageAlpha2

from climatoology.base.computation import (
    ComputationInfo,
    ComputationPluginInfo,
    ComputationResources,
    ComputationScope,
)


@pytest.fixture
def general_uuid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def general_uuid_de() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def default_computation_info(
    general_uuid, default_aoi_feature_geojson_pydantic, default_artifact_enriched, default_plugin_info_final
) -> ComputationInfo:
    return ComputationInfo(
        correlation_uuid=general_uuid,
        request_ts=datetime(2018, 1, 1, 12),
        deduplication_key=uuid.UUID('412bef28-577e-2aa1-5163-77ec18d1acc6'),
        cache_epoch=17532,
        valid_until=datetime(2018, 1, 2),
        params={'id': 1, 'name': 'John Doe', 'execution_time': 0.0, 'option': 'OPT1', 'mapping': {'key': 'value'}},
        requested_params={'id': 1},
        aoi=default_aoi_feature_geojson_pydantic,
        artifacts=[default_artifact_enriched],
        plugin_info=ComputationPluginInfo(
            id=default_plugin_info_final.id,
            version=default_plugin_info_final.version,
            language=default_plugin_info_final.language,
        ),
    )


@pytest.fixture
def default_computation_info_de(
    general_uuid_de, default_aoi_feature_geojson_pydantic, default_artifact_enriched_de, default_plugin_info_final
) -> ComputationInfo:
    return ComputationInfo(
        correlation_uuid=general_uuid_de,
        request_ts=datetime(2018, 1, 1, 12),
        language=LanguageAlpha2('de'),
        deduplication_key=uuid.UUID('c7e7c6e5-af43-9ca3-e02d-de41280fcd0b'),
        cache_epoch=17532,
        valid_until=datetime(2018, 1, 2),
        params={'id': 1, 'name': 'John Doe', 'execution_time': 0.0, 'option': 'OPT1', 'mapping': {'key': 'value'}},
        requested_params={'id': 1},
        aoi=default_aoi_feature_geojson_pydantic,
        artifacts=[default_artifact_enriched_de],
        plugin_info=ComputationPluginInfo(
            id=default_plugin_info_final.id,
            version=default_plugin_info_final.version,
            language=LanguageAlpha2('de'),
        ),
    )


@pytest.fixture
def default_computation_resources(general_uuid) -> Generator[ComputationResources, None, None]:
    with ComputationScope(general_uuid) as resources:
        yield resources
