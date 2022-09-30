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


@pytest.fixture
def principal_df():
    return [1,2,3]

def test_create_dt_as_sql(jinja_env):
    template = jinja_env.get_template('public/interest_rate_test.sql')
    rendered = template.module.interest_rate_test(principal=1, interest_amount=0.5, interest_rate=0.01)
    expected = "(0, 1.0) (1, 1.01) (2, 1.02) (3, 1.03) (4, 1.04)"
    compare(rendered, expected)