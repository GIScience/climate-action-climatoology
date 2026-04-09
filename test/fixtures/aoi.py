import pytest
import shapely
from shapely import Polygon, set_srid

from climatoology.base.computation import AoiFeatureModel, AoiProperties


@pytest.fixture
def default_aoi_geom_shapely() -> shapely.MultiPolygon:
    geom = shapely.MultiPolygon(polygons=[Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.0, 0.0)))])
    srid_geom = set_srid(geometry=geom, srid=4326)
    return srid_geom


@pytest.fixture
def default_aoi_properties() -> AoiProperties:
    return AoiProperties(name='test_aoi', id='test_aoi_id')


@pytest.fixture
def default_aoi_feature_geojson_pydantic(default_aoi_properties) -> AoiFeatureModel:
    return AoiFeatureModel(
        **{
            'type': 'Feature',
            'properties': default_aoi_properties.model_dump(mode='json'),
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [
                    [
                        [
                            [0.0, 0.0],
                            [0.0, 1.0],
                            [1.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ]
                ],
            },
        }
    )


@pytest.fixture
def default_aoi_feature_pure_dict(default_aoi_feature_geojson_pydantic) -> dict:
    return default_aoi_feature_geojson_pydantic.model_dump(mode='json')
