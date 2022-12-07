import argparse
import requests

from scripts.token_checker import TokenChecker

parser = argparse.ArgumentParser()
parser.add_argument('--new_lines')
args = parser.parse_args()
new_lines = args.new_lines.split('/n')
for new_line in new_lines:
    print(f"Validating: {new_line}")
    checker = TokenChecker(new_line=args.new_lines)
    checker.validate_token()