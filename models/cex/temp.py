import re

def convert_hex_to_lowercase(file_path):
    hex_pattern = re.compile(r'0x[a-fA-F0-9]+')

    with open(file_path, 'r') as file:
        content = file.read()

    modified_content = re.sub(hex_pattern, lambda match: match.group(0).lower(), content)

    with open(file_path, 'w') as file:
        file.write(modified_content)

# Usage
file_path = '/Users/hildebert/Documents/gits/dune/spellbook/models/cex/cex_evms_addresses copy.sql'
convert_hex_to_lowercase(file_path)

