import pickle

import faiss
import pytest
import yaml
from langchain import OpenAI
from langchain.chains import RetrievalQAWithSourcesChain

from translation_docstore.prompts import translation_prompt_template

def compare_strings(s1, s2):
    """
    Compare two strings, ignoring whitespace and case.
    Could be a little dangerous if we care about case in addresses, etc?
    """
    s1 = s1.replace(' ', '')
    s2 = s2.replace(' ', '')
    s1 = s1.replace('\n', '')
    s2 = s2.replace('\n', '')
    s1 = s1.lower()
    s2 = s2.lower()
    return s1 == s2


@pytest.fixture
def chain():
    syntax = "spark"
    index = faiss.read_index(f"../docs_{syntax}.index")
    with open(f"../faiss_store_{syntax}.pkl", "rb") as f:
        store = pickle.load(f)
    store.index = index
    chain = RetrievalQAWithSourcesChain.from_chain_type(OpenAI(temperature=0), chain_type="stuff",
                                                        retriever=store.as_retriever())
    return chain


def test_example_alias(chain):
    syntax = "spark"
    snippet = "sum(users) as `daily active users`"
    error = "backquoted identifiers are not supported; use double quotes to quote identifiers at line 1"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], 'sum(users) as "daily active users"')
    assert 'rules/spark/alias_naming.txt' in result_dct['sources']


def test_decimals_for_prices(chain):
    # TODO since prices.tokens exists in DUNE so there is no error
    pass

def test_example_interval(chain):
    syntax = "spark"
    snippet = "WHERE block_time > current_date - Interval '12 month'"
    error = "Io.trino.spi.TrinoException: line 3:35: Unknown resolvedType: Interval at line 3,"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], "WHERE block_time > current_date - Interval '12' month")
    assert 'rules/spark/interval_syntax.txt' in result_dct['sources']

def test_example_reserved_keywords(chain):
    syntax = "spark"
    snippet = "SELECT `from` as wallet_address,"
    error = "backquoted identifiers are not supported; use double quotes to quote identifiers at line 1,"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], 'SELECT "from" as wallet_address,')
    assert 'rules/spark/selecting_keyword_columns.txt' in result_dct['sources']

def test_example_aggregate_fns(chain):
    syntax = "spark"
    snippet = "collect_list(cast(value / 1e18 as decimal(20, 0))) as drops_list"
    error = "Function 'collect_list' not registered at"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], 'array_agg(cast(value / 1e18 as decimal(20, 0))) as drops_list')
    assert 'rules/spark/aggregate_functions.txt' in result_dct['sources']


def test_example_array_fns(chain):
    syntax = "spark"
    snippet = "and array_contains(account_keys, 'feegKBq3GAfqs9G6muPjdn8xEEZhALLTr2xsigDyxnV') = true"
    error = "Function 'array_contains' not registered"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], "and CONTAINS(account_keys, 'feegKBq3GAfqs9G6muPjdn8xEEZhALLTr2xsigDyxnV') = true")
    assert 'rules/spark/array_contains.txt' in result_dct['sources']

def test_example_cast_as_string(chain):
    syntax = "spark"
    snippet = "cast(evt_index as string) as txSignature,"
    error = "Unknown type: string at line"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    ### Sometimes quotes are added around the alias and sometimes not. Both are valid but makes testing difficult.
    assert (compare_strings(result_dct['corrected'], 'cast(evt_index as varchar) as "txSignature",')
    or compare_strings(result_dct['corrected'], 'cast(evt_index as varchar) as txSignature,'))
    assert 'rules/spark/cast_as_string.txt' in result_dct['sources']

def test_example_json_object(chain):
    syntax = "spark"
    snippet = "get_json_object(prizeDistribution, '$.bitRangeSize') AS bitRange"
    error = "Function 'get_json_object' not registered"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    ### Warning: this is a hack to get around the fact that the string prints out with escape characters
    assert compare_strings(result_dct['corrected'], 'json_query(prizeDistribution, \'lax $.bitRangeSize\') AS bitRange')
    assert 'rules/spark/json_objects.txt' in result_dct['sources']

def test_example_percentiles(chain):
    syntax = "spark"
    snippet = 'PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY rolling_balance_final) as "25th_percentile_holdings",'
    error = "mismatched input 'WITHIN'. Expecting: '%', ')', '*', '+', ',', '-', '.', '/', 'AND', 'AT', 'FILTER', 'IGNORE', 'OR', 'OVER', 'RESPECT', '[', '||', <predicate>"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], 'approx_percentile(rolling_balance_final, 0.25) AS "25th_percentile_holdings",')
    assert 'rules/spark/percentiles.txt' in result_dct['sources']

def test_example_substrings(chain):
    syntax = "spark"
    snippet = 'select left(address, 10) as field,'
    error = "mismatched input 'left'. Expecting: '*', 'ALL', 'DISTINCT', <expression>"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert (compare_strings(result_dct['corrected'], 'select substr(address, 1, 10) as field,')
    or compare_strings(result_dct['corrected'], 'select substr(address, 1, 10) as "field",'))
    assert 'rules/spark/returning_substrings.txt' in result_dct['sources']

def test_example_true_false(chain):
    syntax = "spark"
    snippet = 'WHERE call_success is true'
    error = "mismatched input 'true'. Expecting: 'DISTINCT', 'NOT', 'NULL'"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')
    assert result_dct['original'] == snippet
    assert (compare_strings(result_dct['corrected'], 'WHERE call_success = true')
    or compare_strings(result_dct['corrected'], 'WHERE call_success = TRUE'))
    assert 'rules/spark/true_false.txt' in result_dct['sources']

def test_example_unnest(chain):
    syntax = "spark"
    snippet = "select explode(sequence(to_date('2020-12-01'), now(), interval 5 day)) as day"
    error = "mismatched input '1'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', 'AND', 'AT', 'OR', 'ORDER', 'OVER', '[', '||', <predicate>, <string>"
    prompt = translation_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
    result = chain({"question": prompt}, return_only_outputs=True)
    result_dct = yaml.safe_load(f'''
    {result['answer']}
    ''')

    assert result_dct['original'] == snippet
    assert compare_strings(result_dct['corrected'], """with
        day_seq as(
            SELECT
          (
            sequence(
              cast('2020-12-01' as date),
              cast(now() as date),
              interval '5' day
            )
          ) day
      )
    select
      days.day
    from
      day_seq
      cross join unnest(day) as days(day)""")
    assert 'rules/spark/unnest.txt' in result_dct['sources']