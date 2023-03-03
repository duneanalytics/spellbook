import argparse
import json
import logging
import sys
from pathlib import Path

from ruamel.yaml import YAML

logging.basicConfig(stream=sys.stdout, level=logging.WARN)

parser = argparse.ArgumentParser()
parser.add_argument('--model')
args = parser.parse_args()

class SourceSelector(object):
    def __init__(self, method, value):
        self.method = method
        self.value = value


class Sources:
    def __init__(self, model, manifest):
        self.model = model
        self.manifest = manifest
        self.node_dependencies = self.manifest['nodes'][self.model]['depends_on'].get('nodes')
        self.model_dependencies = [model for model in self.node_dependencies if 'model' in model]
        self.source_node_dependencies = [model for model in self.node_dependencies if 'source' in model]
        if self.model_dependencies != []:
            self.subjects = [Sources(model=subject_model, manifest=manifest) for subject_model in
                             self.model_dependencies]
            for subject in self.subjects:
                self.source_node_dependencies.extend(subject.source_node_dependencies)
        else:
            return

def missing_freshness_checks(sources, manifest, check_type='warn'):
    sources = list(set(sources))
    source_defintions = {source: manifest['sources'][source] for source in sources}
    empty_checks = [key for key, value in source_defintions.items() if
                    value['freshness'][f'{check_type}_after']['count'] is None]
    return empty_checks


class MissingSourceFreshnessChecks(Exception):
    def __init__(self, sources_missing_checks):
        self.sources_missing_checks = sources_missing_checks
        self.message = "Sources Missing Freshness Checks: \n" + "\n".join(sources_missing_checks)
        super().__init__(self.message)


with open('../target/manifest.json') as json_file:
    manifest = json.load(json_file)

sources_cls = Sources(model=f'model.spellbook.{args.model}', manifest=manifest)
sources = sources_cls.source_node_dependencies
sources_missing_checks = missing_freshness_checks(sources, manifest)
assert len(sources_missing_checks) == 0, MissingSourceFreshnessChecks(sources_missing_checks)
