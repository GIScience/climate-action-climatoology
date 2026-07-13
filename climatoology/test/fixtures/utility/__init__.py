import pytest
import responses

from climatoology.utility.api import HealthCheck


@pytest.fixture
def mocked_utility_response():
    with responses.RequestsMock() as rsps:
        rsps.get('http://localhost/health', json=HealthCheck().model_dump())
        yield rsps
