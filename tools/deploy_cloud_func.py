#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Template for RDR tool python program.
#
import importlib
import logging
import os
import shutil
import sys
import tempfile

from tools import GCPProcessContext, GCPEnvConfigObject
from aou_cloud.services.gcp_utils import gcp_gcloud_command
from aou_cloud.services.system_utils import git_project_root

_logger = logging.getLogger('aou_cloud')

# Cloud function name and description are required.
tool_cmd = "deploy-func"
tool_desc = "deploy a gcloud function"

PROJECT_BASE_PATH = os.path.abspath(git_project_root())
APP_BASE_PATH = f'{PROJECT_BASE_PATH}/cloud_functions'


class DeployFunctionClass(object):
    """ Deploy a cloud function """
    def __init__(self, args, gcp_env: GCPEnvConfigObject, project_path, func_path):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.project_path = project_path
        self.func_path = func_path

    def _get_deploy_args(self):
        """
        Get the deploy trigger arguments from the function main.py file.
        """
        mod = importlib.import_module(f'cloud_functions.{self.args.function}.main')
        args = mod.get_deploy_args(self.gcp_env)

        return args

    def _prep_for_deploy(self, tmp_path):
        """
        Copy files required for deployment to temp directory.
        :param tmp_path: string with path to temporary deployment directory.
        """
        # Copy the function directory files.
        shutil.copytree(f'{self.func_path}/', f'{tmp_path}/', dirs_exist_ok=True)

        # Copy the 'aou_cloud' directory.
        import aou_cloud as _aou_cloud
        aou_path = os.path.dirname(_aou_cloud.__file__)
        shutil.copytree(f'{aou_path}', f'{tmp_path}/aou_cloud', dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns('__pycache__', 'tools', 'tests'))

        # exclude juypter notebook and aou_cloud libs from requirements.txt file.
        exclude_libs = ['ipykernel', 'ipython', 'python-aou-cloud-services']

        # Read the project root 'requirement.txt' file and build library and dependency list
        lines = open(f'{PROJECT_BASE_PATH}/requirements.txt').readlines()
        # Copy the service requirements.in file and remove excluded libs.
        with open(f'{tmp_path}/requirements.txt', 'w') as h:
            for line in lines:
                if not line.strip() or line.strip().startswith('#'):
                    continue
                # Exclude certain libs from the deploy
                skip_lib = False
                for lib in exclude_libs:
                    if lib in line:
                        skip_lib = True
                if not skip_lib:
                    h.write(line)

        return 0

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        # If running locally, set the active GCP project to 'aou-ehr-ops-curation-test'.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        clr = self.gcp_env.terminal_colors
        # _logger.info(clr.fmt('This is a blue info line.', clr.fg_bright_blue))
        # _logger.info(clr.fmt('This is a custom color line', clr.custom_fg_color(156)))
        _logger.info(clr.fmt('Function Deployment Information:', clr.custom_fg_color(156)))
        _logger.info(clr.fmt(''))
        _logger.info('=' * 90)
        _logger.info('  Target GCP Project    : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Function              : {0}'.format(clr.fmt(self.args.function)))
        _logger.info('=' * 90)

        if not self.args.quiet:
            confirm = input('\nDeploy function (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting deployment.')
                return 1

        trigger_args = ' '.join(self._get_deploy_args())

        # Create a temporary directory to put the function, support code and deploy it.
        tmp_obj = tempfile.TemporaryDirectory(prefix='func_')
        tmp_path = tmp_obj.name

        result = self._prep_for_deploy(tmp_path)

        cwd = os.path.abspath(os.curdir)
        os.chdir(tmp_path)

        if result == 0:

            args = f'deploy {trigger_args}'
            # Add debug logging to cloud function.
            if self.args.debug:
                args += ' --set-env-vars FUNC_DEBUG=1'
            # '--quiet' argument prevents gcloud from asking 'allow unauthenticated requests?' question on cli.
            pcode, so, se = gcp_gcloud_command('functions', args, '--runtime python37 --quiet')

            if pcode == 0:
                _logger.info(f'Successfully deployed function {self.args.function}.')
                if self.args.debug:
                    _logger.info(se or so)
            else:
                _logger.error(f'Failed to deploy function {self.args.function}. ({pcode}: {se or so}).')
                result = -1

        os.chdir(cwd)

        # Clean up temp directory.
        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)

        return result


def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)

    parser.add_argument("--quiet", help="do not ask for user input", default=False, action="store_true")  # noqa
    parser.add_argument('--function', help="gcloud function directory name", required=True)

    args = parser.parse_args()

    func_path = os.path.join(APP_BASE_PATH, args.function)
    if not os.path.exists(func_path):
        raise FileNotFoundError('GCloud function directory not found.')

    with GCPProcessContext(tool_cmd, args) as gcp_env:
        process = DeployFunctionClass(args, gcp_env, APP_BASE_PATH, func_path)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
