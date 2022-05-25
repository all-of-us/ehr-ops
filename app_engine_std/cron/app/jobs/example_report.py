#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import gspread

from aou_cloud.services.gcp_bigquery import BigQueryJob
from aou_cloud.services import gcp_google_sheets as sas
from aou_cloud.services.gcp_google_iam import GCPGooleIAM
from fastapi.responses import JSONResponse
from starlette import status

from ._base_job import BaseCronJob
from services.app_context_base import AppEnvContextBase


_logger = logging.getLogger('aou_cloud')


class ExampleSheetReport:
    """ Populate a spreadsheet report from a BigQuery query """

    def __init__(self, args, gcp_env: AppEnvContextBase):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env: AppEnvContextBase = gcp_env

    def get_or_create_worksheet(self, sh: gspread.Spreadsheet, title, rows=100, cols=26) -> "gspread.Worksheet":
        """
        Get or create the named sheet in the Google Spreadsheet, clear it if needed.
        :param sh: Google spreadsheet object
        :param title: Specific sheet name with in Google spreadsheet.
        :param rows: Number of rows to add to the sheet
        :param cols: Number of columns to add to the sheet
        :return: named sheet object.
        """
        old_ws: gspread.Worksheet = self.gcp_env.retry_func(sh.worksheet, retries=5, title=title)
        if old_ws:
            old_ws.update_title('delete')

        ws = self.gcp_env.retry_func(sh.add_worksheet, title=title, rows=rows, cols=cols)
        if old_ws:
            sh.del_worksheet(old_ws)

        return ws

    def fetch_data(self):
        """ Build and publish the sheet to Google """

        sql = "select * from dev_testing.upload_history order by created"
        job = BigQueryJob(sql, dataset='dev_testing', project=self.gcp_env.project)
        # Note a BigQueryJob returns batches of records, determined by the 'page_size' argument.
        history = list()
        for batch in job:
            for row in batch:
                history.append(row)

        sql = "select * from dev_testing.test_bucket_upload_data order by last_modified"
        job = BigQueryJob(sql, dataset='dev_testing', project=self.gcp_env.project)
        # Note a BigQueryJob returns batches of records, determined by the 'page_size' argument.
        data = list()
        for batch in job:
            for row in batch:
                data.append(row)

        return data, history

    def build_data_tab(self, sh, data):

        ws = self.get_or_create_worksheet(sh, "upload-data", rows=250)
        stor = sas.SheetArrayStorage(ws.id, ws.col_count, ws.row_count)
        ws.clear()

        stor.cells('A1:C1').font(style=sas.FontStyleEnum.bold).column_size(175). \
            horizontal_align(sas.HorizontalAlignEnum.CENTER)
        stor.cells('A2:C2').boarder(sas.BoarderSideEnum.top, sas.Boarder(width=2, color=sas.RGBColor('#666666')))
        stor.cells('A1').value(['Person ID', 'Last Modified', 'Status'])
        # Convert data to list of tuples
        tmp_data = [list(i.to_dict().values()) for i in data]
        stor.cells('A2').value(tmp_data)

        # Push formatting to worksheet.
        body = {'requests': stor.get_formatting()}
        ws.spreadsheet.batch_update(body)
        # Push data to worksheet
        ws.update('A1', stor.to_list())

    def build_history_tab(self, sh, history):

        ws = self.get_or_create_worksheet(sh, "upload-history", rows=250)
        stor = sas.SheetArrayStorage(ws.id, ws.col_count, ws.row_count)
        ws.clear()

        stor.cells('A1:D1').font(style=sas.FontStyleEnum.bold).column_size(200). \
                horizontal_align(sas.HorizontalAlignEnum.CENTER)
        stor.cells('A2:D2').boarder(sas.BoarderSideEnum.top, sas.Boarder(width=2, color=sas.RGBColor('#666666')))
        stor.cells('A1').value(['ID', 'Created', 'Filename', 'Record Count'])
        # Convert data to list of tuples
        tmp_data = [list(i.to_dict().values()) for i in history]
        stor.cells('A2').value(tmp_data)

        # Push formatting to worksheet.
        body = {'requests': stor.get_formatting()}
        ws.spreadsheet.batch_update(body)
        # Push data to worksheet
        ws.update('A1', stor.to_list())

    def run(self):

        app_config = self.gcp_env.get_app_config(project=self.gcp_env.project)
        service_account = self.gcp_env.get_gcp_configurator_account()

        _logger.info('Setting up credentials for google sheet access.')
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform'
        ]
        iam = GCPGooleIAM(scopes)
        service_key_id, service_key_path = iam.create_sa_key(service_account)

        try:
            gc = gspread.service_account(service_key_path, scopes)
            sh = gc.open_by_key(app_config.test_report_sheet_id)

            data, history = self.fetch_data()

            self.build_data_tab(sh, data)
            self.build_history_tab(sh, history)

        except Exception as e:
            _logger.error(e)

        finally:
            iam.delete_sa_key(service_key_id)

class ExampleSheetReportJob(BaseCronJob):
    """ Simple starter template for Cron job """
    # Name is an all lower case url friendly name for the job and should be unique.
    job_name: str = 'example-report-pub'

    def run(self):
        """
        Entry point for cron job.
        :returns: JSONResponse
        """
        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        report = ExampleSheetReport(None, self.gcp_env)
        report.run()

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Job {self.gcp_env.project}.{self.job_name} has completed.')
