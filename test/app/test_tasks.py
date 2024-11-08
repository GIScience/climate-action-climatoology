from unittest.mock import patch


def test_computation_task_init(default_computation_task):
    assert default_computation_task


def test_computation_task_run(default_computation_task, default_artifact, general_uuid):
    with patch('uuid.uuid4', return_value=general_uuid):
        computed_result = default_computation_task.run(params={'id': 1, 'name': 'test'})
    expected_result = [default_artifact.model_dump(mode='json')]
    assert computed_result == expected_result


def test_info_task_init(default_info_task):
    assert default_info_task


def test_info_task_run(default_info_task, default_info, general_uuid):
    computed_result = default_info_task.run()
    expected_result = default_info.model_dump(mode='json')
    assert computed_result == expected_result
