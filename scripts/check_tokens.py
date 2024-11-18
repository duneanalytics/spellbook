import argparse
import logging
import re
import sys
from json import JSONDecodeError
from token_checker import TokenChecker

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('--file_name')
args = parser.parse_args()

# the following pattern supports hexstring address and varchar address
static_record_pattern = r"\('([\w-]+)',\s*'([\w-]+)',\s*'([\w-]+)',\s*(0x[a-fA-F0-9]+|'[\w]+'),\s*(\d+)\),?"
alternative_pattern = r"\('([\w-]+)',\s*'([\w-]+)',\s*(0x[a-fA-F0-9]+|'[\w]+'),\s*(\d+)\),?"

def filter_non_row_lines(new_lines):
    pattern1 = r"\(?'?[\w-]+'?,\s*'?[\w-]+'?,\s*'?[\w-]+'?,\s*(0x[a-fA-F0-9]+|'[\w]+'),\s*\d+\)?"
    pattern2 = r"\(?'?[\w-]+'?,\s*'?[\w-]+'?,\s*(0x[a-fA-F0-9]+|'[\w]+'),\s*\d+\)?"

    filtered_lines = []
    for line in new_lines:
        line = line.strip()
        if line and not line.startswith('--'):
            if bool(re.search(pattern1, line)) or bool(re.search(pattern2, line)):
                filtered_lines.append(line)
    return filtered_lines


with open(f'{args.file_name}') as f:
    new_lines = f.read().strip().split('\n')

cleaned_lines = []
for line in new_lines:
    cleaned_line = line.lstrip('+').strip()
    if cleaned_line:
        cleaned_lines.append(cleaned_line)

filtered_lines = filter_non_row_lines(cleaned_lines)
exceptions = 0

checker = TokenChecker()
for new_line in filtered_lines:
    try:
        checker.validate_token(new_line)
        # Sleep to (hopefully) avoid limits
        # time.sleep(2) (not needed anymore since we fetch everything in batch at the beginning)
    except AssertionError as err:
        exceptions += 1
        logging.error(err)
    except JSONDecodeError as err:
        exceptions += 1
        logging.warning(f'Failed to decode line: {new_line}')
    except Exception as err:
        exceptions += 1
        logging.error(f"Error processing line: {new_line}. {str(err)}")
        raise err

if exceptions > 0:
    raise Exception(
        f"{exceptions} exception/s. Review logs for details. Some could be due simply to missing data from API."
    )