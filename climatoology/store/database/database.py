import logging
from datetime import UTC, datetime, timedelta
from typing import Optional
from uuid import UUID

import geoalchemy2
import geojson_pydantic
from semver import Version
from sqlalchemy import create_engine, select, column, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.ddl import CreateSchema

from climatoology.base.baseoperator import AoiProperties
from climatoology.base.event import ComputationState
from climatoology.base.info import _Info
from climatoology.store.database.tables import (
    PluginAuthorTable,
    InfoTable,
    Base,
    author_info_link_table,
    SCHEMA_NAME,
    ComputationTable,
    ArtifactTable,
    ComputationLookup,
    COMPUTATION_DEDUPLICATION_CONSTRAINT,
)
from climatoology.store.object_store import ComputationInfo, PluginBaseInfo
from climatoology.utility.exception import InfoNotReceivedException, VersionMismatchException

log = logging.getLogger(__name__)


class BackendDatabase:
    def __init__(self, connection_string: str, user_agent: str):
        self.engine = create_engine(
            connection_string, plugins=['geoalchemy2'], connect_args={'application_name': user_agent}
        )

        with self.engine.connect() as connection:
            connection.execute(CreateSchema(SCHEMA_NAME, if_not_exists=True))
            connection.commit()

        Base.metadata.create_all(self.engine)

    def write_info(self, info: _Info, revert: bool = False) -> str:
        log.debug(f'Connecting to the database and writing info for {info.plugin_id}')
        with Session(self.engine) as session:
            self.assert_version_matches(info, revert, session)
            self._synch_info_to_db(info, session)
            session.commit()
            log.info(f'Info written to database for {info.plugin_id}')
            return info.plugin_id

    def _synch_info_to_db(self, info: _Info, session: Session) -> None:
        self._upload_authors(info, session)
        self._upload_info(info, session)
        self._update_info_author_relation_table(info, session)

    def _upload_info(self, info: _Info, session: Session) -> None:
        info_dict = info.model_dump(mode='json', exclude={'authors'})
        info_insert_stmt = (
            insert(InfoTable)
            .values(**info_dict)
            .on_conflict_do_update(index_elements=[InfoTable.plugin_id], set_=info_dict)
        )
        session.execute(info_insert_stmt)

    def _upload_authors(self, info: _Info, session: Session) -> None:
        authors = [author.model_dump(mode='json') for author in info.authors]
        author_insert_stmt = insert(PluginAuthorTable).values(authors).on_conflict_do_nothing()
        session.execute(author_insert_stmt)

    def _update_info_author_relation_table(self, info: _Info, session: Session) -> None:
        session.query(author_info_link_table).filter_by(info_id=info.plugin_id).delete()
        info_author_link = [{'info_id': info.plugin_id, 'author_id': author.name} for author in info.authors]
        link_insert_stmt = insert(author_info_link_table).values(info_author_link).on_conflict_do_nothing()
        session.execute(link_insert_stmt)

    def assert_version_matches(self, info: _Info, revert: bool, session: Session) -> None:
        info_query = session.query(InfoTable).filter_by(plugin_id=info.plugin_id)
        existing_info = info_query.first()
        if existing_info:
            existing_info_version = Version.parse(existing_info.version)
            incoming_info_version = Version.parse(info.version)
            if existing_info_version.compare(incoming_info_version) > 0 and not revert:
                raise VersionMismatchException(
                    f'Refusing to register plugin {info.name} in version {info.version}.'
                    f'A newer version ({existing_info.version}) has previously been registered.'
                    f'Use override to force registration.'
                )

    def read_info(self, plugin_id: str) -> _Info:
        log.debug(f'Connecting to the database and reading info for {plugin_id}')
        with Session(self.engine) as session:
            info_query = select(InfoTable).where(InfoTable.plugin_id == plugin_id)
            result_scalars = session.scalars(info_query)
            result = result_scalars.first()

            if result:
                retrieved_info = _Info.model_validate(result)
                log.debug(f'Info for plugin {plugin_id} read from database')
                return retrieved_info
            else:
                log.error(f'Info for {plugin_id} not available in database')
                raise InfoNotReceivedException()

    def register_computation(
        self,
        correlation_uuid: UUID,
        requested_params: dict,
        aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties],
        plugin_id: str,
        plugin_version: Version,
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
                'timestamp': request_ts,
                'cache_epoch': cache_epoch,
                'valid_until': valid_until,
                'requested_params': requested_params,
                'aoi_geom': aoi.geometry.wkt,
                'aoi_name': aoi.properties.name,
                'aoi_id': aoi.properties.id,
                'plugin_id': plugin_id,
                'plugin_version': plugin_version,
                'status': ComputationState.PENDING,
                'artifact_errors': {},
            }
            computation_insert_stmt = (
                insert(ComputationTable)
                .values(**computation)
                .on_conflict_do_update(
                    constraint=COMPUTATION_DEDUPLICATION_CONSTRAINT,
                    # this is a hack (with currently irrelevant side effects) due to https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql:
                    set_={'plugin_id': plugin_id},
                )
                .returning(column('correlation_uuid'))
            )
            insert_return = session.execute(computation_insert_stmt)
            (db_correlation_uuid,) = insert_return.first()

            lookup_insert_stmt = insert(ComputationLookup).values(
                user_correlation_uuid=correlation_uuid, request_ts=request_ts, computation_id=db_correlation_uuid
            )
            session.execute(lookup_insert_stmt)
            session.commit()

        return db_correlation_uuid

    def read_computation(self, correlation_uuid: UUID) -> Optional[ComputationInfo]:
        with Session(self.engine) as session:
            computation_query = (
                select(ComputationLookup)
                .where(ComputationLookup.user_correlation_uuid == correlation_uuid)
                .options(joinedload(ComputationLookup.computation, innerjoin=True))
            )
            result_scalars = session.scalars(computation_query)
            result = result_scalars.first()

            if result:
                computation_info = result.computation
                computation_info.aoi = geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](
                    **{
                        'type': 'Feature',
                        'properties': {'name': computation_info.aoi_name, 'id': computation_info.aoi_id},
                        'geometry': geoalchemy2.shape.to_shape(computation_info.aoi_geom),
                    }
                )
                computation_info.plugin_info = PluginBaseInfo(
                    plugin_id=computation_info.plugin_id, plugin_version=str(computation_info.plugin.version)
                )
                computation_info = ComputationInfo.model_validate(computation_info)
                log.debug(f'Computation {correlation_uuid} read from database')
                return computation_info
            else:
                log.warning(f'Correlation {correlation_uuid} does not exist in database')
                return None

    def resolve_computation_id(self, user_correlation_uuid: UUID) -> UUID:
        with Session(self.engine) as session:
            resolve_query = select(ComputationLookup.computation_id).where(
                ComputationLookup.user_correlation_uuid == user_correlation_uuid
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
            timestamp=computation_info.timestamp,
            status=computation_info.status,
            artifact_errors=computation_info.artifact_errors,
            message=computation_info.message,
        )
        if invalidate_cache:
            updated_values['cache_epoch'] = None
            updated_values['valid_until'] = computation_info.timestamp

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

    def update_failed_computation(self, correlation_uuid: str, failure_message: Optional[str], cache: bool) -> None:
        timestamp = datetime.now(UTC).replace(tzinfo=None)
        computation_update_stmt = (
            update(ComputationTable)
            .where(ComputationTable.correlation_uuid == correlation_uuid)
            .values(
                timestamp=timestamp,
                cache_epoch=0 if cache else None,
                valid_until=datetime.max if cache else timestamp,
                status=ComputationState.FAILURE,
                message=failure_message,
            )
        )
        with Session(self.engine) as session:
            session.execute(computation_update_stmt)
            session.commit()
