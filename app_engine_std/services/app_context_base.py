#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import time
from typing import Any, Dict, Sequence, List

import psycopg
from psycopg.cursor import BaseCursor
from psycopg.rows import DictRow, RowMaker, no_result

from aou_cloud.services.system_utils import JSONObject
from aou_cloud.environment.gcp import GCPEnvConfigBase

_logger = logging.getLogger('aou_cloud')


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
    app_config = None  # Copy of the app config
    db_config = None  # Copy of database config
    db_conn = None  # Psycopg database connection object if connected.
    db_connections: [List[psycopg.Connection], None] = None  # List of database connections

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_connections = list()

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

    def get_db_instance_info(self, replica=False, project=None, instance_pool='primary'):
        """
        Find matching database instance info from the database config information.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id.
        :return: instance_info object
        """
        project = self.project if not project else project
        if self.project != project or not self.db_config:
            db_config = self.get_db_config(project)
        else:
            db_config = self.db_config
        if not db_config:
            raise ValueError(f'Failed to retrieve database configuration for project: {project}.')

        instance = None
        for item in db_config.instances:
            # Only look at instances that match the given instance name.
            if item.pool == instance_pool:
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
            except Exception as e:
                if count == 0:
                    _logger.error(e)
                    raise e
                time.sleep(2.0 + backoff)
                count -= 1
                backoff += backoff_amount
        return result

    @staticmethod
    def is_valid_db_identifier(self, i, name):
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

    def connect_sqlalchemy_engine(self, database='postgres', replica=False, user='ehr_ops', project=None,
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

    def db_connect_database(self, database='drc', replica=False, user='ehr_ops', project=None,
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
    def db_fetch_all(self, sql, args=None, db_conn=None):
        """
        Run database query and fetch all results..
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: list of query results
        """
        ...

    # pylint: disable=unused-argument
    def db_fetch_one(self, sql, args=None, db_conn=None):
        """
        Run database query and combine query results with column names.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: query results
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