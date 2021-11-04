"""
Restores masked variables in 'EHR Ops Dashboard.twb' workbook from the '.cipher' file
"""

import jinja2
from pathlib import Path
from dotenv import dotenv_values

CIPHER_FILE = '.cipher'
WORKBOOKS = [Path(__file__).parent / 'EHR Ops Dashboard.twb',
                Path(__file__).parent / 'Data_Transfer_Rate.twb']


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