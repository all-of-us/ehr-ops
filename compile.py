import argparse
from pathlib import Path

from jinja2 import FileSystemLoader, Environment, meta
import os


COMPILE_DIR = '.compiled'

_template_loader = FileSystemLoader(searchpath="./")
_template_env = Environment(loader=_template_loader)


def get_parser():
    parser = argparse.ArgumentParser(description='Get some files according to a pattern')
    parser.add_argument('input_files', type=str, action='store', nargs='+', help='Query templates')
    return parser


def get_compile_parser(template_files):
    compile_args = set()
    for template_file in template_files:
        tpl_vars = vars_from_template_file(template_file)
        compile_args = compile_args.union(tpl_vars)
    parser = _parser_from_args(compile_args)
    return parser


def _parser_from_args(compile_args):
    parser = argparse.ArgumentParser(description='Get values for template variables')
    for compile_arg in compile_args:
        parser.add_argument(f'--{compile_arg}', type=str, action='store', required=True)
    return parser


def vars_from_template_file(filename):
    with open(filename, 'r', encoding='utf-8') as fp:
        text = fp.read()
        ast = _template_env.parse(source=text)
        return meta.find_undeclared_variables(ast)


def do_compile(template_files, compile_dir=COMPILE_DIR, **kwargs):
    for template_file in template_files:
        with open(template_file, 'r', encoding='utf-8') as fp:
            template = _template_env.get_template(template_file)
            result = template.render(kwargs)
            output_path = os.path.join(COMPILE_DIR, template_file)
            output_dir = os.path.dirname(output_path)
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as out_fp:
                out_fp.write(result)


if __name__ == '__main__':
    _parser = get_parser()
    args, unknown = _parser.parse_known_args()
    _compile_parser = get_compile_parser(args.input_files)
    _compile_args = _compile_parser.parse_args(unknown)
    do_compile(args.input_files, **vars(_compile_args))
