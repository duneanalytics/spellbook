import argparse
import logging
import re
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('--file_name')
args = parser.parse_args()


def filter_non_row_lines(new_lines):
    filtered_lines = [line for line in new_lines if bool(re.search(r'\((.*?,.*?)\)', line)) and not line.startswith('--')]
    return filtered_lines


with open(f'{args.file_name}') as f:
    new_lines = f.read().strip().split('\n')
cleaned_lines = [new_line.lstrip('+').strip() for new_line in new_lines]
filtered_lines = filter_non_row_lines(cleaned_lines)
exceptions = 0

for new_line in filtered_lines:
    try:
        if re.search('^((?!--).)*from\s+[a-z0-9_]+\.+[a-z0-9_]+', new_line):
            exceptions += 1
            logging.error(f'Invalid reference syntax: {new_line}')
    except Exception as err:
        raise err

if exceptions > 0:
    raise Exception(
        f"{exceptions} exception/s. Review logs for details. Please rewrite these lines in to valid source/ref() format.")
