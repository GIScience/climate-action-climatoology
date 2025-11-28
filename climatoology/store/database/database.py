from datetime import UTC, datetime, timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import geoalchemy2
import geojson_pydantic
from alembic.command import check
from alembic.config import Config
from alembic.util.exc import CommandError
from semver import Version
from sqlalchemy import NullPool, column, create_engine, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, joinedload

from climatoology.base.artifact import ArtifactEnriched
from climatoology.base.computation import AoiProperties, ComputationInfo
from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import PluginAuthor, PluginBaseInfo, PluginInfo
from climatoology.store.database import migration
from climatoology.store.database.models.artifact import ArtifactTable
from climatoology.store.database.models.base import ClimatoologyTableBase
from climatoology.store.database.models.computation import (
    COMPUTATION_DEDUPLICATION_CONSTRAINT,
    ComputationLookupTable,
    ComputationTable,
    PluginInfoTable,
)
from climatoology.store.database.models.info import (
    PluginAuthorTable,
    plugin_info_author_link_table,
)
from climatoology.store.exception import InfoNotReceivedError

log = get_climatoology_logger(__name__)

DEMO_SUFFIX = '-demo'


class BackendDatabase:
    def __init__(self, connection_string: str, user_agent: str, assert_db_status: bool = False):
        self.engine = create_engine(
            connection_string,
            plugins=['geoalchemy2'],
            connect_args={'application_name': user_agent},
            poolclass=NullPool,
        )
        if assert_db_status:
            self.assert_db_status()

    def assert_db_status(self):
        with StringIO() as stdout_replacement:
            alembic_cfg = Config(stdout=stdout_replacement)
            alembic_cfg.set_main_option('script_location', str(Path(migration.__file__).parent))
            alembic_cfg.attributes['connection'] = self.engine
            try:
                check(config=alembic_cfg)
            except CommandError as e:
                log.error(
                    'The target database is not compatible with the expectations by climatoology. Make sure to '
                    'update your database e.g. by running the alembic migration or contacting your admin.',
                    exc_info=e,
                )
                raise e
            finally:
                log.info(stdout_replacement.readlines())

    def write_info(self, info: PluginInfo) -> str:
        log.debug(f'Connecting to the database and writing info for {info.id}')
        with Session(self.engine) as session:
            info_key = self._synch_info_to_db(info, session)
            session.commit()
            log.info(f'Info written to database for {info_key}')
            return info_key

    def _synch_info_to_db(self, info: PluginInfo, session: Session) -> str:
        self._upload_authors(authors=info.authors, session=session)
        info_key = self._upload_info(info=info, session=session)
        self._update_info_author_relation_table(info_key=info_key, authors=info.authors, session=session)
        return info_key

    def _upload_info(self, info: PluginInfo, session: Session) -> str:
        info_update_stmt = (
            update(PluginInfoTable).where(PluginInfoTable.id == info.id, PluginInfoTable.latest).values(latest=False)
        )
        session.execute(info_update_stmt)
        info_dict = info.model_dump(mode='json', exclude={'authors'})
        info_dict['latest'] = True
        info_insert_stmt = (
            insert(PluginInfoTable)
            .values(**info_dict)
            .on_conflict_do_update(index_elements=[PluginInfoTable.key], set_=info_dict)
            .returning(column('key'))
        )
        insert_return = session.execute(info_insert_stmt)
        info_key = insert_return.scalar_one()
        return info_key

    def _upload_authors(self, authors: list[PluginAuthor], session: Session) -> None:
        authors = [author.model_dump(mode='json') for author in authors]
        author_insert_stmt = insert(PluginAuthorTable).values(authors).on_conflict_do_nothing()
        session.execute(author_insert_stmt)

    def _update_info_author_relation_table(self, info_key: str, authors: list[PluginAuthor], session: Session) -> None:
        # In development, where the same version may be written again and an update may be preferred:
        # - we delete old author-info-links for the current info_key to ensure the author seats are created correctly
        # - authors will be accumulated IF they EVER change during a development cycle
        session.query(plugin_info_author_link_table).filter_by(info_key=info_key).delete()
        info_author_link = [
            {'info_key': info_key, 'author_id': author.name, 'author_seat': seat} for seat, author in enumerate(authors)
        ]
        link_insert_stmt = insert(plugin_info_author_link_table).values(info_author_link).on_conflict_do_nothing()
        session.execute(link_insert_stmt)

    def read_info_key(self, plugin_id: str, plugin_version: Optional[Version] = None) -> Optional[str]:
        key_stmt = select(PluginInfoTable.key).where(PluginInfoTable.id == plugin_id)
        if plugin_version:
            key_stmt = key_stmt.where(PluginInfoTable.version == plugin_version)
        else:
            key_stmt = key_stmt.where(PluginInfoTable.latest)
        with Session(self.engine) as session:
            result = session.execute(key_stmt).scalar_one_or_none()
        return result

    def read_info(self, plugin_id: str, plugin_version: Version = None) -> PluginInfo:
        """Read the plugin info from the database.

        :param plugin_id: the id for the plugin of interest
        :param plugin_version: the plugin version to query the info for. Defaults to the latest version
        :return: An _Info object for the plugin
        """
        log.debug(f'Connecting to the database and reading info for {plugin_id}')

        info_key = self.read_info_key(plugin_id=plugin_id, plugin_version=plugin_version)
        if not info_key:
            log.error(f'Info for {plugin_id} not available in database')
            raise InfoNotReceivedError()

        with Session(self.engine) as session:
            info_query = select(PluginInfoTable).where(PluginInfoTable.key == info_key)
            result_scalars = session.scalars(info_query)
            result = result_scalars.one()
            retrieved_info = PluginInfo.model_validate(result)
            log.debug(f'Info for plugin {plugin_id} read from database')
            return retrieved_info

    def register_computation(
        self,
        correlation_uuid: UUID,
        requested_params: dict,
        aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties],
        plugin_key: str,
        computation_shelf_life: Optional[timedelta],
    ) -> UUID:
        with Session(self.engine) as session:
            request_ts = datetime.now(UTC).replace(tzinfo=None)
            if computation_shelf_life is None:  # cache forever
                cache_epoch = 0
                valid_until = datetime.max
            elif computation_shelf_life == timedelta(0):  # don't cache
                cache_epoch = None
                valid_until = request_ts
            else:
                unix_time_zero = datetime.fromtimestamp(0, tz=UTC).replace(tzinfo=None)
                cache_epoch = (request_ts - unix_time_zero) // computation_shelf_life
                valid_until = unix_time_zero + (cache_epoch + 1) * computation_shelf_life

            computation = {
                'correlation_uuid': correlation_uuid,
                'cache_epoch': cache_epoch,
                'valid_until': valid_until,
                'requested_params': requested_params,
                'aoi_geom': aoi.geometry.wkt,
                'plugin_key': plugin_key,
                'artifact_errors': {},
            }
            computation_insert_stmt = (
                insert(ComputationTable)
                .values(**computation)
                .on_conflict_do_update(
                    constraint=COMPUTATION_DEDUPLICATION_CONSTRAINT,
                    # this is a hack (with currently irrelevant side effects) due to https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql:
                    set_={'plugin_key': plugin_key},
                )
                .returning(column('correlation_uuid'))
            )
            insert_return = session.execute(computation_insert_stmt)
            (db_correlation_uuid,) = insert_return.first()

            lookup_insert_stmt = insert(ComputationLookupTable).values(
                user_correlation_uuid=correlation_uuid,
                request_ts=request_ts,
                computation_id=db_correlation_uuid,
                aoi_name=aoi.properties.name,
                aoi_id=aoi.properties.id,
            )
            session.execute(lookup_insert_stmt)
            session.commit()

        return db_correlation_uuid

    def read_computation(self, correlation_uuid: UUID) -> Optional[ComputationInfo]:
        with Session(self.engine) as session:
            computation_query = (
                select(ComputationLookupTable)
                .where(ComputationLookupTable.user_correlation_uuid == correlation_uuid)
                .options(joinedload(ComputationLookupTable.computation, innerjoin=True))
            )
            result_scalars = session.scalars(computation_query)
            result = result_scalars.first()

            if result:
                computation_info = result.computation
                computation_info.request_ts = result.request_ts
                computation_info.aoi = geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](
                    **{
                        'type': 'Feature',
                        'properties': {'name': result.aoi_name, 'id': result.aoi_id},
                        'geometry': geoalchemy2.shape.to_shape(computation_info.aoi_geom),
                    }
                )
                computation_info.plugin_info = PluginBaseInfo(
                    id=computation_info.plugin.id,
                    version=computation_info.plugin.version,
                )
                computation_info = ComputationInfo.model_validate(computation_info)
                log.debug(f'Computation {correlation_uuid} read from database')
                return computation_info
            else:
                log.warning(f'Correlation {correlation_uuid} does not exist in database')
                return None

    def list_artifacts(self, correlation_uuid: UUID) -> List[ArtifactEnriched]:
        computation_info = self.read_computation(correlation_uuid=correlation_uuid)
        return computation_info.artifacts

    @lru_cache(maxsize=256)
    def resolve_computation_id(self, user_correlation_uuid: UUID) -> Optional[UUID]:
        with Session(self.engine) as session:
            resolve_query = select(ComputationLookupTable.computation_id).where(
                ComputationLookupTable.user_correlation_uuid == user_correlation_uuid
            )
            result_scalars = session.scalars(resolve_query)
            return result_scalars.first()

    def add_validated_params(self, correlation_uuid: UUID, params: dict) -> None:
        computation_update_stmt = (
            update(ComputationTable).where(ComputationTable.correlation_uuid == correlation_uuid).values(params=params)
        )
        with Session(self.engine) as session:
            session.execute(computation_update_stmt)
            session.commit()

    def update_successful_computation(self, computation_info: ComputationInfo, invalidate_cache: bool = False) -> None:
        updated_values = dict(
            artifact_errors=computation_info.artifact_errors,
            message=computation_info.message,
        )
        if invalidate_cache:
            updated_values['cache_epoch'] = None
            updated_values['valid_until'] = computation_info.request_ts

        artifacts = [artifact.model_dump(mode='json') for artifact in computation_info.artifacts]
        artifact_insert_stmt = insert(ArtifactTable).values(artifacts)

        computation_update_stmt = (
            update(ComputationTable)
            .where(ComputationTable.correlation_uuid == computation_info.correlation_uuid)
            .values(updated_values)
        )
        with Session(self.engine) as session:
            session.execute(artifact_insert_stmt)
            session.execute(computation_update_stmt)
            session.commit()

    def update_failed_computation(self, correlation_uuid: UUID, failure_message: Optional[str], cache: bool) -> None:
        timestamp = datetime.now(UTC).replace(tzinfo=None)
        computation_update_stmt = (
            update(ComputationTable)
            .where(ComputationTable.correlation_uuid == correlation_uuid)
            .values(
                cache_epoch=0 if cache else None,
                valid_until=datetime.max if cache else timestamp,
                message=failure_message,
            )
        )
        with Session(self.engine) as session:
            session.execute(computation_update_stmt)
            session.commit()


def row_to_dict(row: ClimatoologyTableBase) -> dict:
    result = dict(row.__dict__)
    result.pop('_sa_instance_state', None)
    return result
