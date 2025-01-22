# tmp.txt contains the list of references to be converted to sources. The first item on each comma delimited line is the ref name, the second is the schema, and the last is the alias
# open the file and create a dictionary of the references to a tuple (schema name, alias)

# tmp.txt was created by running the following command:
# dbt -q ls --resource-type model --output json | jq --slurp | jq -r '.[] | "\(.name),\(.config.schema),\(.config.alias)"' | sort -u > tmp.txt


import os
import re


# open the file and create a dictionary of the references to a tuple (schema name, alias)
def create_ref_dict():
    ref_dict = {}
    with open('tmp.txt', 'r') as f:
        for line in f:
            line = line.strip()
            line = line.split(',')
            ref_dict[line[0]] = (line[1], line[2])
    return ref_dict

# parse all .sql in the spellbook directoryand its subdirectories then replace references with sources
def parse_spellbook(ref_dict):
    for root, dirs, files in os.walk('models'):
        for file in files:
            if file.endswith('.sql'):
                with open(os.path.join(root, file), 'r') as f:
                    filedata = f.read()
                for ref in ref_dict:
                    # regex to replace `ref('<ref>')` with `source('<schema>', '<alias>')`. 
                    # Note that there maybe spaces between the ref and the parenthesis and between
                    # the parenthesis and the ref name. Also, the ref name may be enclosed in double quotes

                    filedata = re.sub(r'(\s*)ref(\s*)\((\s*)[\'\"]' + ref + '[\'\"](\s*)\)', r'\1source\3(' + '\'' + ref_dict[ref][0] + '\', \'' + ref_dict[ref][1] + '\')', filedata)
                    # filedata = filedata.replace(ref, ref_dict[ref][0] + '.' + ref_dict[ref][1])
                with open(os.path.join(root, file), 'w') as f:
                    f.write(filedata)

if __name__ == '__main__':
    parse_spellbook(create_ref_dict())