#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import traceback
import re
import time
from typing import Any, Optional, Dict, Sequence, List

import psycopg
from psycopg.cursor import BaseCursor  # pylint: disable=unused-import
from psycopg.rows import DictRow, RowMaker, no_result  # pylint: disable=unused-import
from python_easy_json import JSONObject

from aou_cloud.environment.gcp import GCPEnvConfigBase

_logger = logging.getLogger('aou_cloud')


class DBInstanceInfo(JSONObject):
    """ Model for database instance info from the db_config data. """
    name: str = None
    pool: str = None
    connection_name: str = None
    is_readonly: bool = None


class DBUserInfo(JSONObject):
    """ Model for database user info from the db_config data """
    name: str = None
    instance_pools: List
    is_admin: bool = None
    user: str = None
    password: str = None
    databases: List


class DBVersionModel(JSONObject):
    """
    Simple example JSONObject model to represent the database version in connect_database.
    The class_row_factory() factory returns one or more JSONObjects, where the columns and data
    are turned into a class.  Simple models can be created from creating a subclass of the
    JSONObject class.
    """
    version:str = None


def class_row_factory(cursor: "BaseCursor[Any, DictRow]") -> "RowMaker[JSONObject]":
    """
    Psycopg row factory to represent rows as python class objects, using the JSONObject class.
    The dictionary keys are taken from the column names of the returned columns.
    """
    desc = cursor.description
    if desc is None:
        return no_result

    titles = [c.name for c in desc]

    def dict_row_(values: Sequence[Any]) -> Dict[str, Any]:
        return JSONObject(dict(zip(titles, values)))

    return dict_row_


class AppEnvContextBase(GCPEnvConfigBase):
    """ Shared base class for App Engine Std Context """
    project = None  # Current GCP project

    app_config = None  # Copy of the app config
    db_config = None  # Copy of database config

    db_connections: [List[psycopg.Connection], None] = None  # List of database connections

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_connections = list()

    @staticmethod
    def print_traceback(e):
        """
        Print the traceback of a caught exception.
        :param e: Exception object caught in 'except' clause.
        """
        if not e:
            return
        # Print traceback if we have one.
        tb = e.__traceback__ if hasattr(e, '__traceback__') else None
        if tb:
            e_error = e.__repr__().strip()
            tb_data = ''.join(traceback.format_tb(tb)).strip()
            _logger.error(f'Traceback (most recent call last):\n{tb_data}\n{e_error}')
        else:
            _logger.error(e)

    def override_project(self, project=None):
        """
        Override the Google project id, only when running on local machine.
        :param project: A valid GCP project id.
        """
        if not project or project == 'localhost':
            raise ValueError('Invalid GCP project argument.')

        if self.project == 'localhost':
            from aou_cloud.services.gcp_utils import gcp_set_project
            gcp_set_project(project)
            self.project = project

        return self.project

    def get_db_instance_info(self, replica=False, project=None, instance_pool='primary',
                             name='primary') -> DBInstanceInfo:
        """
        Find matching database instance info from the database config information.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id.
        :param name: specific instance name in pool
        :return: DBInstanceInfo object
        """
        project = self.project if not project else project
        if self.project != project or not self.db_config:
            db_config = self.get_db_config(project)
        else:
            db_config = self.db_config
        if not db_config:
            raise ValueError(f'Failed to retrieve database configuration for project: {project}.')

        instance: Optional[DBInstanceInfo] = None
        for item in db_config.instances:
            # Only look at instances that match the given instance name.
            if item.pool == instance_pool and item.name == name:
                if item.is_readonly == replica:
                    instance = item
                # Pick any instance in the pool as a default
                if not instance:
                    instance = item
        if not instance:
            raise ValueError(f'Unable to match config database instance.')

        if not self.db_config and self.project == project:
            self.db_config = db_config

        return instance

    def get_db_user_info(self, instance: DBInstanceInfo, user: str) -> Optional[DBUserInfo]:
        """
        Find a matching database user info record for the given instance and user name.
        :param instance: DBInstanceInfo instance info object returned from self.get_db_instance_info()
        :param user: user id to match.
        :return: Return DBUserInfo user information object
        """
        if not instance:
            raise ValueError('Invalid database instance info argument')

        db_config = self.db_config or self.get_db_config(self.project)

        user_info: Optional[DBUserInfo] = None
        for item in db_config.users:
            # Only use users that are valid for the instance pool.
            if instance.pool in item.instance_pools and item.name == user:
                user_info = item
                break

        return user_info

    @staticmethod
    def retry_func(func, retries=25, backoff_amount=2.0, **kwargs):
        """
        Retry a function call multiple times, delaying more each retry.
        :param func: function to retry
        :param retries: Integer, number of retries
        :param backoff_amount: Float, number of seconds to add to backoff for next retry.
        :return: function return
        """
        result = None
        count = retries
        backoff = 0.0
        while count > 0:
            try:
                result = func(**kwargs)
                break
            except Exception as e:  # pylint: disable=broad-except
                if count == 0:
                    _logger.error(e)
                    raise e
                time.sleep(2.0 + backoff)
                count -= 1
                backoff += backoff_amount
        return result

    @staticmethod
    def is_valid_db_identifier(i, name):
        """ Validate database object identifier """
        if not all(c.isalpha() or c.isnumeric() or c == '_' for c in i):
            _logger.error(f'{name} name may only contain characters, numbers or underscores.')
            return False
        if len(i) > 63:
            _logger.error(f'{name} name must be 63 bytes or less.')
            return False
        if i[0].isnumeric():
            _logger.error(f'{name} name must start with a letter or underscore.')
            return False
        return True

    @staticmethod
    def convert_to_db_identifier(name):
        """ Convert database object identifier """
        table_name = name.lower()
        table_name = re.sub('([^a-z0-9_]+)','', table_name)
        table_name = table_name[0:62]
        if table_name[0].isnumeric():
            table_name = '_' + table_name

        return table_name

    # pylint: disable=unused-argument
    def connect_sqlalchemy_engine(self, database='postgres', replica=False, user='pdr_ops', project=None,
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
        ...

    # pylint: disable=unused-argument
    def db_connect_database(self, database='drc', replica=False, user='pdr_ops', project=None,
                         instance_pool='primary', row_factory=class_row_factory) -> psycopg.Connection:
        """
        Async: Connect to a PostgreSQL database instance.
        :param database: database name
        :param user: user name defined in database config for this project.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id
        :param row_factory: psycopg row factory function
        """
        ...

    # pylint: disable=unused-argument
    def db_execute(self, sql, args=None, db_conn=None):
        """
        Run database query that returns no results.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        """
        ...

    # pylint: disable=unused-argument
    def db_fetch_many(self, sql, args=None, chunk_size: int=2000, db_conn=None, row_factory=None):
        """
        Return a python generator that returns a chunk of records.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param chunk_size: Number of records to return in each chunk.
        :param db_conn: Database connection object (optional)
        :param row_factory: psycopg compatible row factory
        :return: list of query results
        """
        ...

    # pylint: disable=unused-argument
    def db_fetch_all(self, sql, args=None, db_conn=None, row_factory=None):
        """
        Run database query and fetch all results..
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :param row_factory: psycopg compatible row factory
        :return: list of query results
        """
        ...

    # pylint: disable=unused-argument
    def db_fetch_one(self, sql, args=None, db_conn=None, row_factory=None):
        """
        Run database query and combine query results with column names.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :param row_factory: psycopg compatible row factory
        :return: query results
        """
        ...

    # pylint: disable=unused-argument
    def db_fetch_scalar(self, sql, args=None, db_conn=None):
        """
        Run a query and return a single value from the first row and column in the result.
        Useful for 'select count(1) from ...' statements.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: return single value result
        """
        ...

    # pylint: disable=unused-argument
    def get_database_list(self):
        """ Return the list of database names for the current connection """
        ...

    # pylint: disable=unused-argument
    def get_database_users(self):
        """ Return all the database users, always excludes 'postgres' user and any system users. """
        ...

    # pylint: disable=unused-argument
    def get_database_roles(self):
        """ Return all database roles, always excludes 'postgres' role and any system roles. """
        ...

    # pylint: disable=unused-argument
    def get_database_schemas(self):
        """ Return all database roles, always excludes 'postgres' role and any system roles. """
        ...