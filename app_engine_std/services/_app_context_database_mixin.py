#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

import psycopg
from psycopg.rows import tuple_row

from python_easy_json import JSONObject
from .app_context_base import AppEnvContextBase, class_row_factory, DBVersionModel

_logger = logging.getLogger('aou_cloud')


class AppContextDatabaseMixin(AppEnvContextBase):
    """ Add non-async database connection support to App Context Manager """
    sa_engine = None  # SQLAlchemy database engine object.
    sa_conn = None  # SQLAlchemy database connection object.

    def cleanup(self):
        """ Clean up database connections """
        super().cleanup()

        if self.sa_engine:
            self.sa_conn = None
            self.sa_engine.dispose(close=True)
            self.sa_engine = None

        if self.db_connections:
            for db_conn in self.db_connections:
                if db_conn.closed is False:
                    db_conn.close()
            self.db_connections = list()

    def db_connect_database(self, database='drc', replica=False, user='pdr_ops', project=None,
                         instance_pool='primary', name='primary', row_factory=class_row_factory) -> psycopg.Connection:
        """
        Connect to a PostgreSQL database instance. All connections are stored in the self.db_connections property
        and closed on exit.
        :param database: database name
        :param user: user name defined in database config for this project.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id
        :param name: specific instance name in pool
        :param row_factory: psycopg compatible row factory
        """
        project = self.project if not project else project
        # Find instance we should try to connect to.
        instance = self.get_db_instance_info(replica=replica, project=project, instance_pool=instance_pool, name=name)

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

        db_conn = psycopg.connect(
            user=user_info.name,
            password=user_info.password,
            dbname=database if database else 'postgres',
            host=proxy_conn.host,
            port=proxy_conn.port or 5432,  # Default is standard postgres port.
            row_factory=row_factory if row_factory else tuple_row
        )
        db_conn.autocommit = True
        # Save connection object to connections list, so we can clean the up automatically later.
        self.db_connections.append(db_conn)

        with db_conn.cursor() as cursor:
            cursor.execute('SELECT version() as version;')
            row = cursor.fetchone()
            if isinstance(row, tuple):
                _logger.debug(f'Connected to {instance.connection_name} ({row[0]})')
            else:
                # Cast JSONObject to a known model class
                row: DBVersionModel = JSONObject(row) if isinstance(row, dict) else row
                _logger.debug(f'Connected to {instance.connection_name} ({row.version})')

        return db_conn

    def connect_sqlalchemy_engine(self, database='postgres', replica=False, user='pdr_ops', project=None):
        """
        Create and return a SQL Alchemy engine object connected to self.db_conn.
        :param database: database name
        :param user: user name defined in database config for this project.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :return: SQL Alchemy engine object.
        """
        # self.db_cleanup(async_conn=False)
        # def _get_connection():
        #     return self.db_conn if self.db_conn else self.connect_database(database, replica, user, project)
        #
        # self.sa_engine = create_engine('postgresql://', creator=_get_connection)
        #
        # # TODO: Setup some sort of Session object context manager using 'sessionmaker()',
        # #       So we can run something like 'with self.gcp_env.sa.session() as session`.
        # # RDR Code: 'self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)'
        # self.sa_conn = self.sa_engine.connect()
        # return self.sa_engine
        raise EnvironmentError('Requires psycopg2 library, will not work with newer psycopg library yet.')

    def db_close(self, db_conn: psycopg.Connection):
        """
        Close and remove connection from connections array.
        :param db_conn: psycopg Connection object.
        """
        if db_conn.closed is False:
            db_conn.close()

        found = False
        for conn in self.db_connections:
            if conn == db_conn:
                found = True
                break
        if found is True:
            _logger.debug('Closed and removed db connection from connections array.')
            self.db_connections.remove(db_conn)
            return

        _logger.warning('Unable to find db connection in connections array.')

    def db_execute(self, sql, args=None, db_conn=None):
        """
        Non-async: Run database query that returns no results.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        with db_conn.cursor() as cursor:
            cursor.execute(sql, args)

    def db_fetch_many(self, sql, args=None, chunk_size: int = 2000, db_conn=None, row_factory=None):
        """
        Non-async: Return a python generator that returns a chunk of records.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param chunk_size: Number of records to return in each chunk.
        :param db_conn: Database connection object (optional)
        :param row_factory: psycopg compatible row factory
        :return: list of query results
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        with db_conn.cursor(row_factory=row_factory) as cursor:
            cursor.execute(sql, args)
            data = cursor.fetchmany(chunk_size)
            while data:
                yield data
                data = cursor.fetchmany(chunk_size)

    def db_fetch_all(self, sql, args=None, db_conn=None, row_factory=None):
        """
        Non-async: Run database query and combine query results with column names.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :param row_factory: psycopg compatible row factory
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        with db_conn.cursor(row_factory=row_factory) as cursor:
            cursor.execute(sql, args)
            data = cursor.fetchall()
            return data

    def db_fetch_one(self, sql, args=None, db_conn=None, row_factory=None):
        """
        Non-async: Run database query and combine query results with column names.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :param row_factory: psycopg compatible row factory
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        with db_conn.cursor(row_factory=row_factory) as cursor:
            cursor.execute(sql, args)
            data = cursor.fetchone()
            return data

    def db_fetch_scalar(self, sql, args=None, db_conn=None):
        """
        Run a query and return a single value from the first row and column in the result.
        Useful for 'select count(1) from ...' statements.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: return single value result
        """
        data = self.db_fetch_one(sql, args, db_conn)
        if data:
            if isinstance(data, (list, tuple)):
                return data[0]
            elif isinstance(data, JSONObject):
                return list(data.to_dict().values())[0]
            elif isinstance(data, dict):
                return list(data.values())[0]
            return data
        return None

    def get_database_list(self):
        """ Return the list of database names for the current connection """
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false and " + \
              "datname != 'cloudsqladmin' and datname != 'postgres';"
        databases = self.db_fetch_all(sql)
        return databases

    def get_database_users(self):
        """
        Return all the database users, always excludes 'postgres' user and any system users.
        """
        sql = "select usename from pg_user where usename not like 'cloudsql%' and usename not like 'postgres';"
        users = self.db_fetch_all(sql)
        return users

    def get_database_roles(self):
        """
        Return all database roles, always excludes 'postgres' role and any system roles.
        """
        sql = "select rolname from pg_roles where rolname not like 'cloudsql%' and " + \
              "rolname not like 'pg_%' and rolname not like 'postgres';"
        roles = self.db_fetch_all(sql)
        return roles

    def get_database_schemas(self):
        """
        Return all database roles, always excludes 'postgres' role and any system roles.
        """
        sql = "SELECT schema_name FROM information_schema.schemata " + \
              "WHERE schema_name NOT IN ('pg_catalog', 'information_schema');"
        schemas = self.db_fetch_all(sql)
        return schemas