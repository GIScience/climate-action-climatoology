import logging

from semver import Version
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from climatoology.base.info import _Info
from climatoology.store.database.tables import PluginAuthorTable, InfoTable, Base, author_info_link_table
from climatoology.utility.exception import InfoNotReceivedException, VersionMismatchException

log = logging.getLogger(__name__)


class BackendDatabase:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
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
            result = session.execute(info_query)
            result_scalars = result.scalars()
            result = result_scalars.first()

            if result:
                retrieved_info = _Info.model_validate(result)
                log.debug(f'Info for plugin {plugin_id} read from database')
                return retrieved_info
            else:
                log.error(f'Info for {plugin_id} not available in database')
                raise InfoNotReceivedException()
