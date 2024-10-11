import uuid

import pytest

from climatoology.base.computation import ComputationScope
from climatoology.utility.exception import InputValidationError


def test_operator_typing(default_operator, default_computation_resources):
    default_operator.compute_unsafe(default_computation_resources, {'id': 1234, 'name': 'test'})

    with pytest.raises(InputValidationError):
        default_operator.compute_unsafe(default_computation_resources, {'id': 'ID:1234', 'name': 'test'})


def test_operator_scope():
    correlation_uuid = uuid.uuid4()
    with ComputationScope(correlation_uuid) as resources:
        assert resources.correlation_uuid == correlation_uuid
        assert resources.computation_dir.exists()

    assert not resources.computation_dir.exists()
