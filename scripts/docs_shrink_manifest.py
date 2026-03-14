import json
import sys

from pathlib import Path


class ManifestEditor:
    def __init__(self):
        with open(Path('../target/manifest.json')) as f:
            self.manifest_dict = json.load(f)

    @staticmethod
    def filter_large_raw_sql(node, max_lines=500):
        raw_code = node.get('raw_code', '')
        compiled_code = node.get('compiled_code', '')

        if raw_code.count('\n') > max_lines:
            node['raw_code'] = ''.join(
                raw_code.split('\n')[0:max_lines])

        if compiled_code.count('\n') > max_lines:
            node['compiled_code'] = ''.join(
                compiled_code.split('\n')[0:max_lines])

        return node

    def slim_manifest(self):

        new_nodes = {name: self.filter_large_raw_sql(
            node) for name, node in self.manifest_dict["nodes"].items()}
        self.manifest_dict["nodes"] = new_nodes

    def write_new_manifest(self):
        with open('../target/manifest.json', 'w') as f:
            f.write(json.dumps(self.manifest_dict))

    def main(self):
        self.slim_manifest()
        self.write_new_manifest()


editor = ManifestEditor()
editor.main()
