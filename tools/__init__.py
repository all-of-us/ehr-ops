#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import argparse
import logging
import os
import time
import traceback
import sys

import psycopg
from psycopg.cursor import BaseCursor
from psycopg.rows import DictRow, RowMaker, tuple_row, no_result
from typing import Any, Dict, Sequence, List

from aou_cloud.environment.gcp import GCPEnvConfigBase
from aou_cloud.services.gcp_utils import gcp_cleanup, gcp_initialize
from aou_cloud.services.system_utils import remove_pidfile, write_pidfile_or_die, git_project_root, \
    TerminalColors, setup_logging, JSONObject

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


class GCPEnvConfigObject(GCPEnvConfigBase):
    """ PDR GCP environment configuration object """

    app_config = None  # Copy of the app config
    db_config = None  # Copy of the db config

    git_project = None
    terminal_colors = None

    db_connections: [List[psycopg.Connection], None] = None  # List of database connections

    def __init__(self, items, args):
        """
        :param items: dict of config key value pairs
        :param args: command line arguments from argparse.
        """
        super().__init__(items, args)

        # Determine the git project root directory.
        envron_path = os.environ.get('PDR_PROJECT', None)
        git_root_path = git_project_root()
        if envron_path:
            self.git_project = envron_path
        elif git_root_path:
            self.git_project = git_root_path
        else:
            _logger.warning("GCPEnvConfigObject: no git project root found.")

        self.db_connections = list()

        # Turn on terminal colors.
        self.terminal_colors = TerminalColors()
        clr = self.terminal_colors

        clr.set_default_formatting(clr.bold, clr.custom_fg_color(43))
        clr.set_default_foreground(clr.custom_fg_color(152))
        _logger.info('')

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

    @staticmethod
    def retry_func(func, retries=25, backoff_amount=2.0, **kwargs):
        """
        Retry a calling a python function multiple times, delaying more each retry.
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

    def cleanup(self):
        """ Clean up database connections """
        if self.db_connections:
            for db_conn in self.db_connections:
                if db_conn.closed is False:
                    db_conn.close()
            self.db_connections = list()
        # This should be execute last in this method.
        super().cleanup()

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

    def db_connect_database(self, database='drc', replica=False, user='ehr_ops', project=None,
                            instance_pool='primary', row_factory=class_row_factory) -> psycopg.Connection:
        """
        Connect to a PostgreSQL database instance. All connections are stored in the self.db_connections property
        and closed on exit.
        :param database: database name
        :param user: user name defined in database config for this project.
        :param replica: Connect to writable instance if False else connect to read-only instance.
        :param project: override project used for connection.
        :param instance_pool: db config instance pool id
        :param row_factory: psycopg Connection object
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
                _logger.info(f'Connected to {instance.connection_name} ({row[0]})')
            else:
                # Cast JSONObject to a known model class
                row: DBVersionModel = row
                _logger.info(f'Connected to {instance.connection_name} ({row.version})')

        return db_conn

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

    def db_fetch_all(self, sql, args=None, db_conn=None):
        """
        Non-async: Run database query and combine query results with column names.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        with db_conn.cursor() as cursor:
            cursor.execute(sql, args)
            data = cursor.fetchall()
            return data

    def db_fetch_one(self, sql, args=None, db_conn=None):
        """
        Non-async: Run database query and combine query results with column names.
        :param sql: SQL statement
        :param args: List of statement argument values
        :param db_conn: Database connection object (optional)
        :return: list of JSONObject records
        """
        if not db_conn:
            if not self.db_connections:
                raise IOError('No database connection available to use, please call db_connect_database() first.')
            db_conn = self.db_connections[0]

        with db_conn.cursor() as cursor:
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
            if isinstance(data, JSONObject):
                return list(data.to_dict().values())[0]
            if isinstance(data, dict):
                return list(data.values())[0]
            return data
        return None

    @staticmethod
    def _scalar_list_from_results(data):
        """
        Return list of scalar values from db result set.
        :param data: result from any 'db_fetch_...' method.
        """
        result = list()
        for item in data:
            if isinstance(item, (list, tuple)):
                result.append(item[0])
            elif isinstance(item, JSONObject):
                result.append(list(item.to_dict().values())[0])
            elif isinstance(item, dict):
                result.append(list(item.values())[0])
            else:
                result.append(item)
        return result

    def get_database_list(self):
        """ Return the list of database names for the current connection """
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false and " + \
              "datname != 'cloudsqladmin' and datname != 'postgres';"
        databases = self._scalar_list_from_results(self.db_fetch_all(sql))
        return databases

    def get_database_schemas(self):
        """
        Return all database roles, always excludes 'postgres' role and any system roles.
        """
        sql = "SELECT schema_name FROM information_schema.schemata " + \
              "WHERE schema_name NOT IN ('pg_catalog', 'information_schema');"
        schemas = self._scalar_list_from_results(self.db_fetch_all(sql))
        return schemas

    def get_database_users(self):
        """
        Return all the database users, always excludes 'postgres' user and any system users.
        """
        sql = "select usename from pg_user where usename not like 'cloudsql%' and usename not like 'postgres';"
        users = self._scalar_list_from_results(self.db_fetch_all(sql))
        return users

    def get_database_roles(self):
        """
        Return all database roles, always excludes 'postgres' role and any system roles.
        """
        sql = """
                select rolname from pg_roles 
                where rolname not like 'cloudsql%' and rolname not like 'pg_%' and rolname not like 'postgres';
            """
        roles = self._scalar_list_from_results(self.db_fetch_all(sql))
        return roles

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


class GCPProcessContext(object):
    """
    A processing context manager for GCP operations
    """
    _tool_cmd = None
    _command = None
    _project = 'localhost'  # default to localhost.
    _account = None
    _service_account = None
    _env = None
    _args = None
    _working_dir = None

    _env_config_obj = None

    def __init__(self, command, args, working_dir=None):
        """
        Initialize GCP Context Manager
        :param command: command name
        :param args: parsed argparser commandline arguments object.
        :param working_dir: Directory in project process should be started from.
        """
        if not command:
            _logger.error("command not set, aborting.")
            exit(1)

        self._command = command
        self._project = args.project
        self._account = args.account
        self._service_account = args.service_account
        self._args = args
        self._working_dir = working_dir

        write_pidfile_or_die(command)

        self._env = gcp_initialize(self._project, self._account, self._service_account)
        if not self._env:
            remove_pidfile(command)
            exit(1)

    def __enter__(self):
        """ Return object with properties set to config values """

        os.environ['CONFIG_BASE'] = os.path.join(git_project_root(), '.configs')

        if self._working_dir:
            # Set working directory.
            wd = os.path.join(git_project_root(), self._working_dir)
            os.chdir(wd)
            os.environ['PYTHONPATH'] = os.path.abspath(wd)

        self._env_config_obj = GCPEnvConfigObject(self._env, self._args)
        return self._env_config_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Clean up or close everything we need to """

        if self._working_dir:
            os.chdir(git_project_root())

        self._env_config_obj.cleanup()
        gcp_cleanup(self._account)
        remove_pidfile(self._command)

        if exc_type is not None:
            print((traceback.format_exc()))
            _logger.error("program encountered an unexpected error, quitting.")
            exit(1)

    @staticmethod
    def setup_logging(tool_cmd):
        setup_logging(
            _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None)

    @staticmethod
    def get_argparser(tool_cmd, tool_desc):
        """
        :param tool_cmd: Tool command line id.
        :param tool_desc: Tool description.
        """
        # Setup program arguments.
        parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
        parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")
        parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")
        parser.add_argument("--project", help="gcp project name", default="localhost")
        parser.add_argument("--account", help="pmi-ops account", default=None)
        parser.add_argument("--service-account", help="gcp iam service account", default=None)
        # Allow using an existing Cloud SQL Proxy connection instead of starting a new one.
        parser.add_argument("--port", help="port of an existing cloud sql proxy connection.", type=int, default=None)
        return parser

    @staticmethod
    def update_argument(sub_parser, dest, help=None):  # pylint: disable=redefined-builtin
        """
        Update sub-parser argument description and choices.
        :param sub_parser: argparse subparser object.
        :param dest: Destination property where argument value is stored.  IE: 'file_name' == args.file_name.
        :param help: Set help text for dest argument.
        """
        if not sub_parser or not dest:
            raise ValueError('Arguments must include a sub-parser and dest string.')
        for a in sub_parser._actions:
            if a.dest == dest:
                a.help = help
