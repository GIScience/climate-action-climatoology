import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterable, AsyncIterator
from unittest.mock import patch, AsyncMock, Mock

import pytest

from climatoology.base.event import ComputeCommandStatus
from climatoology.broker.message_broker import AsyncRabbitMQ


@pytest.fixture()
def connection():
    mocked_pool = AsyncMock()

    class ChannelPool(AsyncIterable):

        def __aiter__(self) -> AsyncIterator:
            return self

        async def __anext__(self):
            raise StopAsyncIteration()

        @staticmethod
        def acquire():
            return mocked_pool

    broker = AsyncRabbitMQ(host='rabbitmq.test.org',
                           port=9999,
                           user='user',
                           password='password')
    broker.channel_pool = ChannelPool

    yield {
        'mocked_pool': mocked_pool,
        'broker': broker
    }


@pytest.mark.asyncio
async def test_publish_status_update(connection):
    await connection['broker'].publish_status_update(uuid.uuid4(), ComputeCommandStatus.COMPLETED, 'success')
    channel = await connection['mocked_pool'].__aenter__()
    exchange = await channel.declare_exchange()
    exchange.publish.assert_called_once()


def iterator(body: str):
    @asynccontextmanager
    class TestAsyncIterator(AsyncIterable):

        def __init__(self, *args, **kwargs):
            self.call_count = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.call_count == 0:
                return self
            raise StopAsyncIteration()

        @property
        def body(self):
            self.call_count += 1
            return body

        @asynccontextmanager
        def process(self):
            return self

    return TestAsyncIterator


@pytest.mark.asyncio
async def test_request_info(connection, default_info):
    channel = await connection['mocked_pool'].__aenter__()

    queue_mock = AsyncMock()
    queue_mock.iterator = iterator(default_info.model_dump_json())
    channel.declare_queue.return_value = queue_mock

    info = await connection['broker'].request_info(plugin_name='test_plugin')
    assert info == default_info


@pytest.mark.asyncio
async def test_send_compute(connection):
    await connection['broker'].send_compute(plugin_name='test_plugin', params={}, correlation_uuid=uuid.uuid4())
    channel = await connection['mocked_pool'].__aenter__()
    mocked_exchange = channel.default_exchange
    mocked_exchange.publish.assert_called_once()
