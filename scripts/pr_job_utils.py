import argparse
import json
import subprocess
from pathlib import Path

from string import Template


class PRJobDepedencyManager:
    def __init__(self, pr_schema: str):
        self.pr_schema = pr_schema
        self.manifest_dict = json.load(open(Path('../target/manifest.json')))
        self.nodes = {'.'.join(k.split('.')[0:3]):v for k,v in self.manifest_dict["nodes"].items()}

    @staticmethod
    def get_names_from_bash(bash_response, object_type):
        if "Runtime Error" in bash_response:
            raise Exception(bash_response)
        if 'No nodes selected!' in bash_response:
            modified_objects = []
        elif 'does not match any nodes' in bash_response:
            modified_objects = []
        else:
            modified_names = bash_response.split('\n')
            modified_names.remove('')
            modified_objects = [f"{object_type}.spellbook.{name}" for name in modified_names]
        return modified_objects

    def fetch_object_keys_by_state(self, object_type, state):
        """
        Collected keys for objects by state
        :param object_type:  accepted inputs: [model, seed]
        :return: modified_objects
        """
        # Test fork is messy because I have not found the syntax to apply two selectors at once using dbt list
        if object_type == 'test':

            #Select all tests so we can intersect with tests that require seeds
            bash_response_all_modified = subprocess.run(
                f'dbt list --output name --resource-type test --select state:{state} --state  .',
                capture_output=True, shell=True).stdout.decode("utf-8")
            modified_tests = self.get_names_from_bash(bash_response_all_modified, object_type)

            #Seclect generic tests that use seeds
            bash_response_test_that_use_seeds = subprocess.run(
                f'dbt list --output name --resource-type test  --select config.materialized:seed',
                capture_output=True, shell=True).stdout.decode("utf-8")
            test_that_use_seeds = self.get_names_from_bash(bash_response_test_that_use_seeds, object_type)

            # Select only custom tests
            bash_response_custom_modified = subprocess.run(
                f'dbt list --output name --resource-type test --select state:{state} --state  .  --exclude  test_type:generic',
                capture_output=True, shell=True).stdout.decode("utf-8")
            modified_custom_tests = self.get_names_from_bash(bash_response_custom_modified, object_type)

            modified_objects = list(set(modified_tests).intersection(set(test_that_use_seeds)).union(modified_custom_tests))
        else:
            bash_response = subprocess.run(
                f'dbt list --output name --resource-type {object_type} --select state:{state} --state  .  --exclude  test_type:generic',
                capture_output=True, shell=True).stdout.decode("utf-8")
            modified_objects = self.get_names_from_bash(bash_response, object_type)
        return modified_objects

    def fetch_new_object_keys(self, object_type):
        """
        Collected keys for new objects
        :param object_type:  accepted inputs: [model, test, seed]
        :return: modified_objects
        """
        return self.fetch_object_keys_by_state(object_type, state='new')

    def fetch_modified_object_keys(self, object_type):
        """
        Collected keys for modified objects
        :param object_type:  accepted inputs: [model, test, seed]
        :return: modified_objects
        """
        return self.fetch_object_keys_by_state(object_type, state='modified')

    def fetch_modified_node_keys(self):
        models = self.fetch_modified_object_keys(object_type="model")
        seeds = self.fetch_modified_object_keys(object_type="seed")
        tests = self.fetch_modified_object_keys(object_type="test")
        modified_node_keys = models + seeds + tests
        return modified_node_keys

    def parse_manifest_for_nodes(self, models):
        """
        Use model names to filter manifest for nodes which contain all of the model specifications.
        :param self:
        :param models:
        :return: selected_nodes
        """
        selected_nodes = [self.nodes[model] for model in models]
        return selected_nodes

    def fetch_required_refs(self, modifed_nodes):
        """
        Parse nodes to extract the refs or spellbook models the modified models depend on.
        :param self:
        :param modifed_nodes:
        :return: prod_names
        """
        ref_names = []
        for node in modifed_nodes:
            ref_names.extend(node['depends_on']['nodes'])

        # remove sources
        ref_names = [ref_name for ref_name in ref_names if 'source' not in ref_name]

        # isolate seeds
        seed_names = [ref_name for ref_name in ref_names if 'seed'  in ref_name]

        # remove seeds
        ref_names = [ref_name for ref_name in ref_names if 'seed' not in ref_name]

        new_refs = self.fetch_new_object_keys(object_type='model') + \
                   self.fetch_new_object_keys(object_type='seed') + \
                   self.fetch_new_object_keys(object_type='test')
        modifed_refs = self.fetch_modified_object_keys(object_type='model') + \
                       self.fetch_modified_object_keys(object_type='seed') + \
                       self.fetch_modified_object_keys(object_type='test')

        # Add seeds back in
        ref_names = ref_names + list(set(seed_names))

        # Remove any dependencies that are created in the pr
        for new_ref in (new_refs + modifed_refs):
            ref_names = [ref for ref in ref_names if ref != new_ref]
        # Deduplicate refs
        ref_names = list(set(ref_names))

        return ref_names

    @staticmethod
    def get_prod_name(node):
        """
        Helper function used to parse node and reconstruct the production table name.
        Raises an error if the schema of a dependency is not defined.
        :param node:
        :return: prod_name
        """
        if node['config']['alias'] is None:
            return f"{node['config']['schema']}.{node['name']}"
        if node['config']['schema'] is None:
            raise Exception(f"Schema is not defined for model: {node['name']}")
        return f"{node['config']['schema']}.{node['config']['alias']}"

    def compile_ref_production_names(self, refs):
        """
        Use the alias and schema from the nodes to construct the production table/view name.
        This will raise an error when creating a view if no schema or alias are set and the name was created
        using the file name.
        :param refs:
        :return: prod_names (table or view names used in the production db)
        """
        ref_nodes = self.parse_manifest_for_nodes(refs)
        prod_names = [self.get_prod_name(node) for node in ref_nodes]
        return prod_names

    def compile_pr_job_names(self, refs, modified_nodes):
        """
        Make a list of the table/view names that would be created by the modified model dependencies.
        :param refs:
        :return: pr_names, the table view names that will be used in the PR dbt job
        """
        modified_paths = [node['path'] for node in modified_nodes]
        ref_nodes = self.parse_manifest_for_nodes(refs)
        pr_names = [f"test_schema.{self.pr_schema}_{node['name']}" for node in ref_nodes]
        return pr_names

    def generate_views_file(self, prod_names, pr_names):
        """
        Creates macro to be run after the prod manifest is collected and new manifest is created
        :param prod_names:
        :param pr_names:
        :return: overwrites file "../macros/dune/create_views_of_dependencies.sql"
        """
        f = open("../macros/dune/create_views_of_dependencies.sql", 'w')
        f.write("{% macro create_views_of_dependencies() %}")
        for prod_name, pr_name in list(zip(prod_names, pr_names)):
            view_template = Template("""
{% set $var %}
CREATE OR REPLACE VIEW $pr_name AS
SELECT * FROM $prod_name;
{% endset %}
{% do run_query($var) %}
""")
            view_command = view_template.substitute(var=prod_name.replace('.', ''), prod_name=prod_name,
                                                    pr_name=pr_name)
            f.write(view_command)
        f.write("{% endmacro %}")
        f.close()

    def main(self):
        modified_node_keys = self.fetch_modified_node_keys()
        modified_nodes = self.parse_manifest_for_nodes(modified_node_keys)
        refs = self.fetch_required_refs(modified_nodes)
        prod_names = self.compile_ref_production_names(refs)
        pr_names = self.compile_pr_job_names(refs, modified_nodes)

        self.generate_views_file(prod_names, pr_names)


parser = argparse.ArgumentParser()
parser.add_argument('--pr_schema', help='must match dbt schema defined in profiles.yml file')
args = parser.parse_args()
manager = PRJobDepedencyManager(pr_schema=args.pr_schema)
manager.main()