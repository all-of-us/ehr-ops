"""
Strips out sensitive information from the 'EHR Ops Dashboard.twb' workbook.
Currently, removed information includes:
    1. Several properties in connection tags
    2. The entire extract tag
    3. Project and dataset information in relation tags
"""

from lxml import etree
import re
from dotenv import dotenv_values
from pathlib import Path

CIPHER_FILE = '.cipher'

DASHBOARDS_PATH = Path(__file__).parent / 'dashboard_metrics'
WORKBOOKS = [DASHBOARDS_PATH / 'EHR Ops Dashboard' / 'EHR Ops Dashboard.twb',
             DASHBOARDS_PATH / 'Data Transfer Rate' / 'Data Transfer Rate.twb',
             DASHBOARDS_PATH / 'EHR Ops General Data Quality Dashboard' / 'EHR Ops General Data Quality Dashboard.twb',
             DASHBOARDS_PATH / 'NIH Grant Award Metrics' / 'NIH Grant Award Metrics.twb']

CONNECTION_TAG = 'connection'
EXTRACT_TAG = 'extract'
RELATION_TAG = 'relation'


def strip_connections(xml):
    nullable_attributes = [
        'CATALOG', 'EXECCATALOG', 'project', 'schema', 'username', 'dbname'
    ]
    connections = xml.findall(f'.//{CONNECTION_TAG}')

    for connection in connections:
        connection_attributes = connection.attrib
        for attr in nullable_attributes:
            if attr in connection_attributes:
                connection.attrib[attr] = ''


def strip_extract(xml):
    extracts = xml.findall(f'.//{EXTRACT_TAG}')

    for extract in extracts:
        extract.getparent().remove(extract)


def reverse_dict(d):
    return {value: key for key, value in d.items()}


def contains_dict(s, d):
    for key, _value in d.items():
        if re.search(key, s):
            return True

    return False


def replace_from_dict(s, d):
    for key, value in d.items():
        s = re.sub(key, f'{{{{{value}}}}}', s)

    return s


def strip_relations(xml):
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

            relation.text = replace_from_dict(relation_string, cipher)


def main():
    for workbook in WORKBOOKS:
        print(f"Processing {workbook}...")
        with open(workbook, 'rb') as f:
            xml = etree.XML(f.read())

        print('Stripping connections')
        strip_connections(xml)
        print('Stripping extract')
        strip_extract(xml)
        print('Stripping relations')
        strip_relations(xml)

        with open(workbook, 'wb') as f:
            f.write(etree.tostring(xml, pretty_print=True))


if __name__ == '__main__':
    main()
