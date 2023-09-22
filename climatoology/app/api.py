import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List
from uuid import UUID

import hydra
import uvicorn
from fastapi import APIRouter, FastAPI, WebSocket, HTTPException, WebSocketException
from fastapi.responses import FileResponse
from hydra import compose
from pika.exceptions import ChannelClosedByBroker

from climatoology.base.event import ComputeCommandStatus, ComputeCommandResult
from climatoology.base.operator import Info, Artifact
from climatoology.broker.message_broker import ManagedRabbitMQ
from climatoology.store.object_store import MinioStorage
from climatoology.utility.exception import InfoNotReceivedException

config_dir = os.getenv('API_GATEWAY_APP_CONFIG_DIR', str(Path('conf').absolute()))


@asynccontextmanager
async def configure_dependencies(app: FastAPI):
    hydra.initialize_config_dir(config_dir=config_dir, version_base=None)
    cfg = compose(config_name='config')

    app.state.storage = MinioStorage(host=cfg.store.host,
                                     port=cfg.store.port,
                                     access_key=cfg.store.access_key,
                                     secret_key=cfg.store.secret_key,
                                     secure=cfg.store.secure == 'True',
                                     bucket=cfg.store.bucket,
                                     file_cache=cfg.store.file_cache)
    app.state.broker = ManagedRabbitMQ(host=cfg.broker.host,
                                       port=cfg.broker.port,
                                       api_url=cfg.broker.api_url,
                                       user=cfg.broker.user,
                                       password=cfg.broker.password)
    yield


health = APIRouter(prefix='/health')
plugin = APIRouter(prefix='/plugin', tags=['plugin'])
computation = APIRouter(prefix='/computation', tags=['computation'])
store = APIRouter(prefix='/store', tags=['store'])

tags_metadata = [
    {
        'name': 'plugin',
        'description': 'Interact with plugins.',
    },
    {
        'name': 'computation',
        'description': 'Inquire about currently running computations.'
                       'This endpoint implements a WebSocket.'
                       'It has one optional parameter (correlation_uuid) that is used to filter updates by.'
                       'The WebSocket will push updates in json format on the status of currently running computations.'
                       'Be aware that you will only be sent future updates. Any historic states are lost.'
                       'If you need the full set of states, make sure to subscribe to all notifications '
                       'and then filter on your side.',
    },
    {
        'name': 'store',
        'description': 'Interact with the content store.',
    },
]

description = """
# Climate Action API Gateway

The API Gateway to the HeiGIT Climate Action platform abstracts all functionally for the user realm.
It servers as the single point of interaction.
"""

app = FastAPI(
    title='Climate Action API Gateway',
    summary='Interact with the stateful Climate Action platform.',
    description=description,
    version='1.0.0',
    contact={
        'name': 'Climate Acton Team',
        'url': 'https://heigit.org/',
        'email': 'climate-action@heigit.org',
    },
    openapi_tags=tags_metadata,
    lifespan=configure_dependencies)


@health.get(path='/',
            status_code=200,
            summary='Hey, is this thing on?')
def is_ok() -> dict:
    return {'status': 'ok'}


@plugin.get(path='/',
            summary='List all currently available plugins.')
def plugins() -> List[Info]:
    return app.state.broker.list_plugins()


@plugin.get(path='/{name}',
            summary='Get information on a specific plugin or check its online status.')
def get_plugin(name: str) -> Info:
    try:
        return app.state.broker.request_info(plugin_name=name)
    except InfoNotReceivedException as e:
        raise HTTPException(status_code=404, detail=f'Plugin {name} does not exist.') from e


@plugin.post(path='/{name}',
             summary='Schedule a computation task on a plugin.',
             description='The parameters depend on the chosen plugin. '
                         'Their input schema can be requested from the /plugin GET methods.')
def plugin_compute(name: str, params: dict) -> UUID:
    correlation_uuid = uuid.uuid4()
    try:
        app.state.broker.send_compute(name, params, correlation_uuid)
    except ChannelClosedByBroker as e:
        if e.reply_code == 404:
            app.state.broker.publish_status_update(correlation_uuid=correlation_uuid,
                                                   status=ComputeCommandStatus.FAILED)
            raise HTTPException(status_code=404, detail='The plugin is not online.') from e
        raise HTTPException from e
    app.state.broker.publish_status_update(correlation_uuid=correlation_uuid,
                                           status=ComputeCommandStatus.SCHEDULED)
    return correlation_uuid


@computation.websocket(path='/')
async def subscribe_compute_status(websocket: WebSocket, correlation_uuid: UUID = None) -> None:
    channel = app.state.broker.get_channel()
    await websocket.accept()

    async def subscribe_callback(ch, method, properties, body):
        status = ComputeCommandResult.model_validate_json(body.decode())
        if not correlation_uuid or status.correlation_uuid == correlation_uuid:
            await websocket.send_json(status.model_dump_json())

    try:
        channel.basic_consume(queue=app.state.broker.get_status_queue(),
                              on_message_callback=subscribe_callback,
                              consumer_tag='WebSocket for compute events',
                              auto_ack=True)

        channel.start_consuming()
    except ChannelClosedByBroker as e:
        raise WebSocketException(code=1003) from e


@store.get(path='/{correlation_uuid}',
           summary='List all artifacts associated with a correlation uuid.',
           description='Note that this list may be emtpy if the computation has not been started '
                       'or is not yet completed. '
                       'To receive actual content you need to use the store uuid returned.')
def list_artifacts(correlation_uuid: UUID) -> List[Artifact]:
    return app.state.storage.list_all(correlation_uuid=correlation_uuid)


@store.get(path='/{correlation_uuid}/{store_uuid}',
           summary='Download a specific file.',
           description='The store uuid can be parsed from the listing endpoint.'
                       'It is advised to also provide the filename in order to get a proper file type.')
def fetch_artifact(correlation_uuid: UUID, store_uuid: UUID, file_name: Path = None) -> FileResponse:
    file_path = app.state.storage.fetch(correlation_uuid=correlation_uuid,
                                        store_uuid=store_uuid,
                                        file_name=file_name)

    if not file_path and file_name.is_file():
        raise HTTPException(status_code=404, detail=f'The requested element {correlation_uuid}/{store_uuid} does '
                                                    'not exist!')
    return FileResponse(path=file_path)


app.include_router(health)
app.include_router(plugin)
app.include_router(computation)
app.include_router(store)

if __name__ == '__main__':
    uvicorn.run(app,
                host='0.0.0.0',
                port=int(os.getenv('API_GATEWAY_API_PORT', 8000)),
                log_config=f'{config_dir}/logging/app/logging.yaml')
