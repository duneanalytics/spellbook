import argparse
import logging
import re
import sys
import time

from token_checker import TokenChecker

logging.basicConfig(stream=sys.stdout, level=logging.WARN)


parser = argparse.ArgumentParser()
parser.add_argument('--file_name')
args = parser.parse_args()

def filter_non_row_lines(new_lines):
    filtered_lines = [line for line in new_lines if bool(re.search(r'\((.*?,.*?)\)',line))]
    return filtered_lines

with open(f'{args.file_name}') as f:
    new_lines = f.read().strip().split('\n')
cleaned_lines = [new_line.lstrip('+').strip() for new_line in new_lines]
filtered_lines = filter_non_row_lines(cleaned_lines)
exceptions = 0

for new_line in filtered_lines:
    try:
        checker = TokenChecker(new_line=new_line)
        checker.validate_token()
        # Sleep to (hopefully) avoid limits
        time.sleep(2)
    except Exception as err:
        if type(err) != AssertionError:
            if hasattr(err, 'response'):
                if err.response.status_code == 402:
                    logging.warning('Rate limited, sleep for one hour before continue')
                    time.sleep(3600)
            else:
                raise err
        else:
            exceptions+=1
            logging.error(err)
if exceptions > 0:
    raise Exception(f"{exceptions} exception/s. Review logs for details. Some could be due simply to missing data from API.")