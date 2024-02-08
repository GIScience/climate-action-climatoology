import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterable, AsyncIterator
from unittest.mock import AsyncMock

import pytest

from climatoology.base.event import ComputeCommandStatus
from climatoology.broker.message_broker import AsyncRabbitMQ
from climatoology.utility.exception import ClimatoologyVersionMismatchException


@pytest.fixture()
def broker():
    mock = AsyncMock()
    mock.acquire = AsyncMock()

    class MockIter(AsyncIterable):
        def __init__(self, *args, **kwargs):
            self.call_count = 0

        def __aiter__(self) -> AsyncIterator:
            return self

        async def __anext__(self):
            if self.call_count == 0:
                self.call_count = +1
                return mock
            raise StopAsyncIteration()

        def acquire(self):
            return mock

        def channel(self):
            return mock

        def athrow(self, exception: Exception, message: str, traceback):
            raise exception

    @asynccontextmanager
    def ctx():
        return MockIter()

    broker = AsyncRabbitMQ(
        host='rabbitmq.test.org',
        port=9999,
        user='user',
        password='password',
        assert_plugin_version=False,
    )
    mock.acquire = ctx
    mock.channel = ctx

    broker.connection_pool = mock

    yield broker


@pytest.mark.asyncio
async def test_publish_status_update(broker):
    async with broker.connection_pool.acquire() as connection:
        async with connection.channel() as channel:
            await broker.publish_status_update(uuid.uuid4(), ComputeCommandStatus.COMPLETED, 'success')
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

        def athrow(self, exception: Exception, message: str, traceback):
            raise exception

        @property
        def body(self):
            self.call_count += 1
            return body

        @asynccontextmanager
        def process(self):
            return self

    return TestAsyncIterator


@pytest.mark.asyncio
async def test_request_info(broker, default_info):
    async with broker.connection_pool.acquire() as connection:
        async with connection.channel() as channel:
            queue_mock = AsyncMock()
            queue_mock.iterator = iterator(default_info.model_dump_json())
            channel.declare_queue.return_value = queue_mock

            info = await broker.request_info(plugin_id='test_plugin')
            assert info == default_info


@pytest.mark.asyncio
async def test_request_info_plugin_version_assert(broker, default_info):
    broker.assert_plugin_version = True
    async with broker.connection_pool.acquire() as connection:
        async with connection.channel() as channel:
            queue_mock = AsyncMock()
            queue_mock.iterator = iterator(default_info.model_dump_json())
            channel.declare_queue.return_value = queue_mock

            with pytest.raises(ClimatoologyVersionMismatchException):
                await broker.request_info(plugin_id='test_plugin')


@pytest.mark.asyncio
async def test_send_compute(broker):
    async with broker.connection_pool.acquire() as connection:
        async with connection.channel() as channel:
            await broker.send_compute(plugin_id='test_plugin', params={}, correlation_uuid=uuid.uuid4())
            mocked_exchange = channel.default_exchange
            mocked_exchange.publish.assert_called_once()
