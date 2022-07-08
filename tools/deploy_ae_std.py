#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Deploy App to GCP
#
import datetime
import glob
import logging
import os
import shutil
import sys
import tempfile

from aou_cloud.services.gcp_utils import gcp_deploy_app, gcp_restart_instances, gcp_application_default_creds_exist
from aou_cloud.services.jira_utils import JiraTicketHandler
from aou_cloud.services.system_utils import git_project_root, run_external_program, git_current_branch, \
    git_checkout_branch, is_git_branch_clean

from tools import GCPProcessContext, GCPEnvConfigObject


_logger = logging.getLogger('aou_cloud')


# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tools.bash'
tool_cmd = "deploy-std"
tool_desc = "deploy app-engine standard service"

PROJECT_BASE_PATH = os.path.abspath(git_project_root())
APP_BASE_PATH = f'{PROJECT_BASE_PATH}/app_engine_std'


class AppEngineStdDeploy(object):

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.app_path = os.path.join(APP_BASE_PATH, self.args.app)

        self.jira_ready = False
        self.jira_board = 'PD'
        self.jira_handler = None
        self.stable_project_id = 'aou-pdr-data-stable'
        self.production_project = 'aou-pdr-data-prod'
        self.deploy_version = None

        self.git_repo = 'program-data-repository'
        self.git_root = git_project_root()
        self.current_git_branch = git_current_branch()

        self.developers = 'Jason Patterson, Zoey Jiang'
        self.project_manager = 'Justin Cook'

    def create_jira_ticket(self, summary, descr=None, board_id=None):
        """
        Create a Jira ticket.
        """
        if self.args.no_jira is True:
            return None
        # Save current working directory, we can't make 'git' calls in the temporary deploy directory.
        cwd = os.getcwd()
        os.chdir(self.git_root)
        # Get previous tag release from git
        args = ['git', 'tag', '--list']
        code, so, _ = run_external_program(args=args)
        if code != 0:
            os.chdir(cwd)
            raise IOError("Failed to retrieve previous git tag")
        try:
            previous_tag = so.strip().split('\n')[-2]
        except IndexError:
            previous_tag = ''

        if not descr:
            circle_ci_url = '<CircleCI URL>'
            if 'CIRCLE_BUILD_URL' in os.environ:
                circle_ci_url = os.environ.get('CIRCLE_BUILD_URL')
            change_log = self.jira_handler.get_release_notes_since_tag(self.git_repo, previous_tag,
                                                                       self.args.git_target)

            today = datetime.datetime.today()
            descr = f"""h1. Release Notes for {self.args.git_target}
            h2.deployed to {self.gcp_env.project}, listing changes since {previous_tag}:
            {change_log}

            h3. Change Management Description
            System: All of Us DRC, Raw Data Repository (RDR)
            Developers: {self.developers}
            Needed By Date/Event: <target release date>
            Priority: <Low, Medium, High>
            Configuration/Change Manager: {self.project_manager}

            Anticipated Impact: <None, Low, Medium, High>
            Software Impact: <Software Impact>
            Training Impact: <Training Impact>
            Data Impact: <Data Impact>

            Testing
            Tester: {self.developers}
            Date Test Was Completed: {today.strftime("%b %-d, %Y")}
            Implementation/Deployment Date: Ongoing

            Security Impact: <None, Low, Medium, High>

            CircleCI Output: {circle_ci_url}
            """

        if not board_id:
            board_id = self.jira_board

        ticket = self.jira_handler.create_ticket(summary, descr, board_id=board_id)
        os.chdir(cwd)
        return ticket

    def add_jira_comment(self, comment):
        """
        Add a comment to a Jira ticket
        :param comment: Comment to add to Jira ticket.
        """
        if not self.jira_ready or self.args.no_jira is True:
            return

        # If this description changes, change in 'create_jira_roc_ticket' as well.
        summary = f"Release tracker for {self.args.git_target}"
        tickets = self.jira_handler.find_ticket_from_summary(summary, board_id=self.jira_board)

        ticket = None
        if tickets:
            ticket = tickets[0]
        else:
            # Determine if this is a stable environment deploy.
            if self.gcp_env.project == self.stable_project_id:
                ticket = self.create_jira_ticket(summary)
                if not ticket:
                    _logger.error('Failed to create JIRA ticket')
                else:
                    _logger.info(f'Created JIRA ticket {ticket.key} for tracking release.')

        if ticket:
            self.jira_handler.add_ticket_comment(ticket, comment)

        return comment

    def _read_app_service(self):
        """ Read the app.yaml file and grep the service name. """
        yaml = os.path.join(APP_BASE_PATH, self.args.app, 'app.yaml')
        if not os.path.exists(yaml):
            _logger.error(f'Could not find app.yaml file for service {self.args.app}, aborting.')
            return None

        with open(yaml) as h:
            for line in h.readlines():
                if line.startswith('service: '):
                    return line.split(':')[1].strip()

        _logger.error(f'Cloud not find service name in app.yaml file for service {self.args.app}, aborting.')
        return None

    def _copy_service_configs(self, tmp_path):
        """ Update the web service configs with the deploy app path. """
        service_path = os.path.join(self.app_path, '../services')
        dest_path = os.path.join(tmp_path, 'services')
        shutil.copytree(f'{service_path}', dest_path, dirs_exist_ok=True)

        for file in ('supervisor.conf', ):
            lines = open(os.path.join(dest_path, file)).readlines()
            with open(os.path.join(dest_path, file), 'w') as h:
                for line in lines:
                    if 'app.main:app' in line:
                        line = line.replace('app.main:app', f'{self.args.app}.app.main:app')
                    h.write(f'{line}\n')


    def _prep_for_deploy(self, tmp_path):
        """
        Copy files required for deployment to temp directory.
        :param tmp_path: string with path to temporary deployment directory.
        """
        # Copy the app directory files.
        shutil.copytree(f'{self.app_path}/', f'{tmp_path}/{self.args.app}/', dirs_exist_ok=True)

        # Copy any top level yaml files to temp root
        for file in glob.glob(rf'{APP_BASE_PATH}/*.yaml'):
            shutil.copy(file, tmp_path)

        # Move any app yaml files to tmp_path root.
        for file in glob.glob(rf'{tmp_path}/{self.args.app}/*.yaml'):
            shutil.copy(file, tmp_path)

        self._copy_service_configs(tmp_path)

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

    def get_aou_cloud_info(self):
        """
        Return information about the current 'aou-cloud' library package.
        :return: version, path to library
        """
        # import aou_cloud as _aou_cloud
        # aou_path = os.path.dirname(_aou_cloud.__file__)
        # from importlib.metadata import version
        # aou_version = version('aou_cloud')
        # return aou_version, aou_path
        args = ['pip', 'list']
        code, so, se = run_external_program(args)  # pylint: disable=unused-variable
        lines = so.split('\n')
        for line in lines:
            if 'aou-cloud' in line:
                while '  ' in line:
                    line = line.replace('  ', ' ')
                parts = line.split(' ')
                return parts[1], parts[2]
        return None, None

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        # If running locally, set the active GCP project to 'aou-ehr-ops-curation-test'.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        service = self._read_app_service()
        if not service:
            return 1
        setattr(self.args, 'service', service)

        # Installing the app config makes API calls and needs an oauth token to succeed.
        if not self.args.quiet and not gcp_application_default_creds_exist() and not self.args.service_account:
            _logger.error('\n*** Google application default credentials were not found. ***')
            _logger.error("Run 'gcloud auth application-default login' and then try deploying again.\n")
            return 1

        if not is_git_branch_clean():
            _logger.error('*** There are uncommitted changes in current branch, aborting. ***\n')
            return 1

        aou_cloud_ver, aou_cloud_path = self.get_aou_cloud_info()
        if not aou_cloud_ver:
            _logger.error("Could not find the 'aou-cloud' python library path.")
            return 1

        self.deploy_version = self.args.deploy_as or self.args.git_target.replace('.', '-')

        clr = self.gcp_env.terminal_colors
        # _logger.info(clr.fmt('This is a blue info line.', clr.fg_bright_blue))
        # _logger.info(clr.fmt('This is a custom color line', clr.custom_fg_color(156)))
        _logger.info(clr.fmt('App Deployment Information:', clr.custom_fg_color(156)))
        _logger.info(clr.fmt(''))
        _logger.info('=' * 110)
        _logger.info('  AOU Cloud Library     : {0}'.format(clr.fmt(f'v{aou_cloud_ver} @ {aou_cloud_path}')))
        _logger.info('  Target GCP Project    : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Branch/Tag To Deploy  : {0}'.format(clr.fmt(self.args.git_target)))
        _logger.info('  App                   : {0}'.format(clr.fmt(self.args.app)))
        _logger.info('  Deploy As             : {0}'.format(clr.fmt(self.deploy_version)))

        if 'JIRA_API_USER_NAME' in os.environ and 'JIRA_API_USER_PASSWORD' in os.environ and \
                self.args.project in ('aou-pdr-data-stable', 'aou-pdr-data-prod'):
            self.jira_ready = True
            self.jira_handler = JiraTicketHandler(
                os.environ.get('JIRA_API_USER_NAME'), os.environ.get('JIRA_API_USER_PASSWORD'))

        if self.args.no_jira is False:
            if self.jira_ready:
                _logger.info('  JIRA Credentials      : {0}'.format(clr.fmt('Set')))
            else:
                if self.gcp_env.project in ('all-of-us-rdr-prod', 'all-of-us-rdr-stable'):
                    _logger.info('  JIRA Credentials      : {0}'.format(clr.fmt('*** Not Set ***', clr.fg_bright_red)))

        _logger.info('=' * 110)

        if not self.args.quiet:
            confirm = input('\nDeploy App (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting deployment.')
                return 1

        # Change current git branch/tag to git target.
        if not git_checkout_branch(self.args.git_target):
            _logger.error(f'Unable to switch to git branch/tag {self.args.git_target}, aborting.')
            return 1
        _logger.info(f'Switched to {git_current_branch()} branch/tag.')

        # Create a temporary directory to put the app, support code in and deploy it.
        tmp_obj = tempfile.TemporaryDirectory(prefix='ae_')
        tmp_path = tmp_obj.name

        result = self._prep_for_deploy(tmp_path)

        cwd = os.path.abspath(os.curdir)
        os.chdir(tmp_path)

        if result == 0:
            yaml_files = [os.path.basename(f) for f in glob.glob(r'*.yaml')]
            msg = f'Deploying app {self.args.app} (v{self.args.git_target}) to {self.gcp_env.project}.'
            self.add_jira_comment(msg)
            if gcp_deploy_app(self.gcp_env.project, yaml_files, version=self.deploy_version, promote=True) is True:
                msg = f'Completed deploy of app {self.args.app} (v{self.deploy_version}) to {self.gcp_env.project}.'
                _logger.info(msg)
                self.add_jira_comment(msg)
                gcp_restart_instances(self.gcp_env.project, self.args.service)
            else:
                msg = f'Failed to deploy app {self.args.app} (v{self.deploy_version}) to {self.gcp_env.project}.'
                self.add_jira_comment(msg)
                _logger.error(msg)
                result = -1

        os.chdir(cwd)

        # Clean up temp directory.
        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)

        git_checkout_branch(self.current_git_branch)

        return result

def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)

    parser.add_argument('-q', '--quiet', help="do not ask for user input", default=False, action="store_true")
    # The App argument specifies the GCP App Engine service name to deploy.
    parser.add_argument('-a', '--app', help="name of app engine app to deploy", required=True)
    parser.add_argument("--deploy-as", help="deploy as version", default=None)
    parser.add_argument("--git-target", help="git branch/tag to deploy.", default=git_current_branch())
    parser.add_argument("--no-jira", help="do not use any jira services", default=False, action="store_true")
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args) as gcp_env:
        process = AppEngineStdDeploy(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
