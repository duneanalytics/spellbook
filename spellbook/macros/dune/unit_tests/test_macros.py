import os
import re

import jinja2
import pytest


def compare(a, b):
    """
    Compare two base strings, disregarding whitespace
    """
    return re.sub("\s*", "", a) == re.sub("\s*", "", b)


@pytest.fixture
def jinja_env():
    template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
    return jinja_env


def test_get_create_global_temp_view_as_sql(jinja_env):
    template = jinja_env.get_template('create_global_temporary_table.sql')
    rendered = template.module.create_global_temp_view(relation='table_name', sql='select 1')
    expected = "CREATE OR REPLACE GLOBAL TEMPORARY VIEW table_name as ( select 1 ) "
    compare(rendered, expected)


def test_create_dt_as_sql(jinja_env):
    template = jinja_env.get_template('create_dt_as.sql')
    rendered = template.module.create_dt_as(file_path='`/tmp/schema/table`', sql='select 1')
    expected = "CREATE OR REPLACE TABLE delta.`/tmp/schema/table` USING DELTA as ( select 1 )"
    compare(rendered, expected)
