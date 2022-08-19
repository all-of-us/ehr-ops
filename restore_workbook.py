"""
Restores masked variables in 'EHR Ops Dashboard.twb' workbook from the '.cipher' file
"""

import jinja2
from pathlib import Path
from dotenv import dotenv_values

CIPHER_FILE = '.cipher'
DASHBOARDS_PATH = Path(__file__).parent / 'dashboard_metrics'
WORKBOOKS = [DASHBOARDS_PATH / 'EHR Ops Dashboard' / 'EHR Ops Dashboard.twb',
             DASHBOARDS_PATH / 'Data Transfer Rate' / 'Data Transfer Rate.twb',
             DASHBOARDS_PATH / 'EHR Ops General Data Quality Dashboard' / 'EHR Ops General Data Quality Dashboard.twb',
             DASHBOARDS_PATH / 'NIH Grant Award Metrics' / 'NIH Grant Award Metrics.twb']


def load_template(filepath):
    template_loader = jinja2.FileSystemLoader(
        searchpath=str(Path(filepath).parent))
    template_env = jinja2.Environment(loader=template_loader, autoescape=True)
    template = template_env.get_template(Path(filepath).name)

    return template


def main():
    for workbook in WORKBOOKS:
        cipher_dict = dotenv_values(CIPHER_FILE)
        template = load_template(workbook)

        print(f'Filling workbook {workbook} template...')
        filled_template = template.render(**cipher_dict)

        with open(workbook, 'w') as f:
            f.write(filled_template)


if __name__ == '__main__':
    main()