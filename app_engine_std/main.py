#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import argparse
import shlex
import subprocess
import os


# Note: _APP_CHOICES values must be the same as the app directory names under app-engine.std.
GAE_APP_SERVICES = ('cron', 'example')
_SERVICE_CHOICES = ('fastapi', 'gunicorn', 'supervisor')


def print_service_list():
    print("Help:")
    print(f"  possible apps are: {', '.join(GAE_APP_SERVICES)}")
    print(f"  possible services are: {', '.join(_SERVICE_CHOICES)}")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='ae-std-service', description="PDR app engine std micro service")
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")
    parser.add_argument("--service", help="launch micro app service", default='fastapi',
                        choices=_SERVICE_CHOICES, nargs='?', required=True)
    parser.add_argument("--app", help="micro service app to run", default='app',
                        choices=GAE_APP_SERVICES, nargs='?', required=True)
    parser.add_argument("--unittests", help="enable unittest mode", default=False, action="store_true")
    parser.add_argument("-c", "--config", help="path to config directory", default="./.configs")
    args = parser.parse_args()

    if not os.path.exists(args.app):
        print('Error: app path does not exist, is app named correctly?')
        exit(-1)

    config_base = os.path.abspath(args.config)
    if not os.path.exists(config_base):
        print(f'Error: configuration directory is not found ({config_base}).')
        exit(-1)

    # Setup running environment.
    os.environ['CONFIG_BASE'] = config_base
    if args.unittests:
        os.environ["UNITTEST_FLAG"] = "1"
    env = dict(os.environ)

    supervisor_config = f'{args.app}_service/services/supervisor.conf'
    # Change directory to app engine app directory.
    cwd = os.curdir
    os.chdir(args.app)

    if args.service == 'fastapi':
        # Note: not for production environment use.
        p_args = ['uvicorn', 'app.main:app']

    elif args.service == 'gunicorn':
        # Use the gunicorn command line defined for supervisor.
        import configparser

        config = configparser.ConfigParser()
        config.read(supervisor_config)
        command = config['program:fastapi']['command']

        p_args = shlex.split(command)
        # print(p_args)

    else: # supervisor (default):
        p_args = ['supervisord', '-c', supervisor_config]

    p = subprocess.Popen(p_args, env=env)
    p.wait()

    os.chdir(cwd)
    exit(0)
