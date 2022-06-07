#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

import psycopg
from aou_cloud.services.system_utils import JSONObject
from psycopg.rows import tuple_row
from sqlalchemy.ext.asyncio import create_async_engine

from .app_context_base import AppEnvContextBase, class_row_factory, DBVersionModel

_logger = logging.getLogger('aou_cloud')


class AppContextAsyncDatabase(AppEnvContextBase):
    """ Add async aware database connection support to App Context Manager """
    sa_engine = None  # Async aware SQLAlchemy database engine object.
    sa_conn = None  # Async aware SQLAlchemy database connection object.

    async def cleanup(self):
        """ Clean up database connections """
        super().cleanup()

        if self.sa_engine:
            self.sa_conn = None
            await self.sa_engine.dispose(close=True)
            self.sa_engine = None

        if self.db_connections:
            for db_conn in self.db_connections:
                if db_conn.closed is False:
                    await db_conn.close()
            self.db_connections = list()

    async def db_connect_database(self, database='drc', replica=False, user='ehr_ops', project=None,
                         instance_pool='primary', row_factory=class_row_factory):
        """
        Async: Connect to a PostgreSQL database instance. All connections are stored in the self.db_connections
        property and closed on exit.
        :param database: database name
        :param user: user name defined in database config for this project.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id
        :param row_factory: psycopg row factory function
        """
        project = self.project if not project else project
        # Find instance we should try to connect to.
        instance = self.get_db_instance_info(replica=replica, project=project, instance_pool=instance_pool)

        db_config = self.db_config if self.project == project else self.get_db_config(project)

        user_info = None
        for item in db_config.users:
            # Only use users that are valid for the instance pool.
            if instance.pool in item.instance_pools and item.name == user:
                user_info = item
                break
        if not user_info:
            raise ValueError(f'Database user {user} not found in db config.')

        # Find an existing cloud sql proxy connection or start a new connection.
        proxy_conn = self.activate_sql_proxy(instance.connection_name)
        if not proxy_conn:
            raise ValueError('Failed to find or activate a Cloud SQL Proxy connection.')

        # https://www.psycopg.org/psycopg3/docs/advanced/async.html
        import psycopg
        db_conn = await psycopg.AsyncConnection.connect(
            user=user_info.name,
            password=user_info.password,
            dbname=database if database else 'postgres',
            host=proxy_conn.host,
            port=proxy_conn.port or 5432,  # Default is standard postgres port.
            row_factory=row_factory if row_factory else tuple_row
        )
        await db_conn.set_autocommit(True)
        self.db_connections.append(db_conn)

        async with db_conn.cursor() as cursor:
            await cursor.execute('SELECT version() as version;')
            row = await cursor.fetchone()
            if isinstance(row, tuple):
                _logger.info(f'Connected to {instance.connection_name} ({row[0]})')
            else:
                # Cast JSONObject to a known model class
                row: DBVersionModel = row
                _logger.info(f'Connected to {instance.connection_name} ({row.version})')

        return db_conn

    async def connect_sqlalchemy_engine(self, database='postgres', replica=False, user='ehr_ops', project=None,
                                        instance_pool='primary'):
        """
        Async: Create and return a SQL Alchemy engine object connected to self.db_conn.
        :param database: database name
        :param user: user name defined in database config for this project.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id
        :return: SQL Alchemy engine object.
        """
        if self.sa_engine:
            self.sa_conn = None
            await self.sa_engine.dispose()
            self.sa_engine = None
            self.db_conn = None

        async def _get_connection():
            """ create_engine function needs callable function to return database connection. """
            if self.db_conn:
                return self.db_conn
            # row_factory must be the psycopg default of "tuple_row".
            return await self.connect_database(database, replica, user, project, instance_pool, tuple_row)

        self.sa_engine = create_async_engine('postgresql://', creator=_get_connection)
        self.sa_conn = await self.sa_engine.connect()

        return self.sa_engine

    async def db_execute(self, sql, args=None, db_conn=None):
        """
        Async aware: Run database query that returns no results.
        https://www.psycopg.org/psycopg3/docs/advanced/async.html
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        async with db_conn.cursor() as cursor:
            await cursor.execute(sql, args)

    # pylint: disable=unused-argument
    async def db_fetch_many(self, sql, args=None, chunk_size: int = 2000, db_conn=None):
        """
        Async aware: Return a python generator that returns a chunk of records.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param chunk_size: Number of records to return in each chunk.
        :param db_conn: Database connection object (optional)
        :return: list of query results
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        async with db_conn.cursor() as cursor:
            await cursor.execute(sql, args)
            data = cursor.fetchmany(chunk_size)
            while data:
                yield data
                data = cursor.fetchmany(chunk_size)
            raise StopAsyncIteration

    async def db_fetch_all(self, sql, args=None, db_conn=None):
        """
        Async aware, run database query and combine query results with column names.
        https://www.psycopg.org/psycopg3/docs/advanced/async.html
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        async with db_conn.cursor() as cursor:
            await cursor.execute(sql, args)
            return await cursor.fetchall()

    async def db_fetch_one(self, sql, args=None, db_conn=None):
        """
        Async aware, run database query and combine query results with column names.
        https://www.psycopg.org/psycopg3/docs/advanced/async.html
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        async with db_conn.cursor() as cursor:
            await cursor.execute(sql, args)
            return await cursor.fetchone()

    async def db_fetch_scalar(self, sql, args=None, db_conn=None):
        """
        Async aware, run a query and return a single value from the first row and column in the result.
        Useful for 'select count(1) from ...' statements.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: return single value result
        """
        data = await self.db_fetch_one(sql, args, db_conn)
        if data:
            if isinstance(data, (list, tuple)):
                return data[0]
            elif isinstance(data, JSONObject):
                return list(data.to_dict().values())[0]
            elif isinstance(data, dict):
                return list(data.values())[0]
            return data
        return None

    async def get_database_list(self):
        """ Return the list of database names for the current connection """
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false and " + \
              "datname != 'cloudsqladmin' and datname != 'postgres';"
        databases = await self.db_fetch_all(sql)
        return databases

    async def get_database_users(self):
        """ Return all the database users, always excludes 'postgres' user and any system users. """
        sql = "select usename from pg_user where usename not like 'cloudsql%' and usename not like 'postgres';"
        users = await self.db_fetch_all(sql)
        return users

    async def get_database_roles(self):
        """ Return all database roles, always excludes 'postgres' role and any system roles. """
        sql = "select rolname from pg_roles where rolname not like 'cloudsql%' and " + \
              "rolname not like 'pg_%' and rolname not like 'postgres';"
        roles = await self.db_fetch_all(sql)
        return roles

    async def get_database_schemas(self):
        """ Return all database roles, always excludes 'postgres' role and any system roles. """
        sql = "SELECT schema_name FROM information_schema.schemata " + \
              "WHERE schema_name NOT IN ('pg_catalog', 'information_schema');"
        schemas = await self.db_fetch_all(sql)
        return schemas