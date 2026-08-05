from psycopg import Connection


def connection_to_string(connection: Connection) -> str:
    user = connection.info.user
    password = connection.info.password
    host = connection.info.host
    port = connection.info.port
    dbname = connection.info.dbname

    connection_str = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}'

    return connection_str
