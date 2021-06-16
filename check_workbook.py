"""
Checks for and flags sensitive information in the 'EHR Ops Dashboard.twb' workbook.
Currently, flagged information includes:
    1. Several properties in connection tags
    2. The entire extract tag
    3. Project and dataset information in relation tags

"""

from lxml import etree
import re
from dotenv import dotenv_values
import sys
from pathlib import Path
import argparse

CIPHER_FILE = '.cipher'
WORKBOOK = Path(__file__).parent / 'EHR Ops Dashboard.twb'

CONNECTION_TAG = 'connection'
EXTRACT_TAG = 'extract'
RELATION_TAG = 'relation'


def check_connections(xml):
    nullable_attributes = [
        'CATALOG', 'EXECCATALOG', 'project', 'schema', 'username'
    ]
    connections = xml.findall(f'.//{CONNECTION_TAG}')

    for connection in connections:
        for attr in nullable_attributes:
            if attr in connection.attrib and connection.attrib[attr]:
                return False

    return True


def check_extract(xml):
    extracts = xml.findall(f'.//{EXTRACT_TAG}')

    if extracts:
        return False

    return True


def reverse_dict(d):
    return {value: key for key, value in d.items()}


def contains_dict(s, d):
    for key, value in d.items():
        if re.search(key, s):
            return True

    return False


def replace_from_dict(s, d):
    for key, value in d.items():
        s = re.sub(key, f'{{{{{value}}}}}', s)

    return s


def check_relations(xml):
    cipher = reverse_dict(dotenv_values(CIPHER_FILE))
    relations = xml.findall(f'.//{RELATION_TAG}')
    for relation in relations:
        relation_attributes = relation.attrib
        relation_string = relation.text

        for attr_key, attr_value in relation_attributes.items():
            if contains_dict(attr_value, cipher):
                relation.attrib[attr_key] = replace_from_dict(
                    attr_value, cipher)

        if relation_string and contains_dict(relation_string, cipher):
            return False

    return True


def main(check):
    with open(WORKBOOK, 'rb') as f:
        xml = etree.XML(f.read())

    run_all = True if not check else False

    if run_all:
        print('Checking connections...')
        check1 = check_connections(xml)
        print('Checking extract...')
        check2 = check_extract(xml)
        print('Checking relations...')
        check3 = check_relations(xml)

        checks = {
            'connections': check1,
            'extract': check2,
            'relations': check3
        }

        if not all(checks.values()):
            sys.exit(
                f"The following tag checks revealed sensitive data: {[check for check in checks if not checks[check]]}."
            )
        else:
            print("Workbook passed checks.")
    else:
        valid = True
        if check == 'connections':
            print('Checking connections...')
            valid = check_connections(xml)
        elif check == 'extract':
            print('Checking extract...')
            valid = check_extract(xml)
        elif check == 'relations':
            print('Checking relations...')
            valid = check_relations(xml)

        if not valid:
            sys.exit(f"The {check} tag check revealed sensitive data.")
        else:
            print(f"Workbook passed {check} check.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Execute content checks of Tableau workbook XML.")
    parser.add_argument(
        '--check',
        choices=['connections', 'extract', 'relations'],
        help="(optional) Check to run. If blank, defaults to all.")
    args = parser.parse_args()
    check = args.check

    main(check)
