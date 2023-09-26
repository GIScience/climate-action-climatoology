from unittest.mock import patch

import pytest
from pika import BasicProperties

from climatoology.broker.message_broker import InfoCallbackHolder, ManagedRabbitMQ


@pytest.fixture()
def mocked_client():
    with (patch('climatoology.broker.message_broker.BlockingConnection') as rabbitmq_connection,
          patch('climatoology.broker.message_broker.ManagementApi') as rabbitmq_api):
        rabbitmq_broker = ManagedRabbitMQ(host='rabbitmq.test.org',
                                          port=9999,
                                          api_url='rabbitmq.api.org',
                                          user='user',
                                          password='password')

        rabbitmq_api.queue.return_value.list.return_value = ['test_plugin_info']

        yield {'rabbitmq_broker': rabbitmq_broker,
               'rabbitmq_connection': rabbitmq_connection,
               'rabbitmq_api': rabbitmq_api}


def test_getters(mocked_client):
    assert mocked_client['rabbitmq_broker'].get_status_queue() == 'notify'
    assert mocked_client['rabbitmq_broker'].get_compute_queue('test') == 'test_compute'
    assert mocked_client['rabbitmq_broker'].get_info_queue('test') == 'test_info'


# def test_request_info(mocked_client):
#     assert mocked_client['rabbitmq_broker'].request_info('test').name == 'test_plugin'
#
#
# def test_list_plugins(mocked_client):
#     assert len(mocked_client['rabbitmq_broker'].list_plugins()) == 1
#     assert mocked_client['rabbitmq_broker'].list_plugins()[0].name == 'test_plugin'


def test_info_callback(default_info, general_uuid):
    info_callback_holder = InfoCallbackHolder(correlation_uuid=general_uuid)
    info_callback_holder.on_response(None,
                                     None,
                                     BasicProperties(correlation_id=str(general_uuid)),
                                     default_info.model_dump_json())
    assert info_callback_holder.info_return.model_dump_json() == default_info.model_dump_json()
