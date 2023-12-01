{{
  config(
    schema = 'compound_v3_base',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{% set config_ctokens %}
  select asset_symbol, comet_contract_address, collateral_token_address, asset_address
  from (values
    ('WETH', 0x46e6b214b524310239732D51387075E0e70970bf, 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 0x4200000000000000000000000000000000000006), --cbETH
    ('USDbC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA), --cbETH
    ('USDbC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x4200000000000000000000000000000000000006, 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA)  --WETH
  ) as x (asset_symbol, comet_contract_address, collateral_token_address, asset_address)
{% endset %}

{%
  set config_sources = [
    {'decoded_project': 'compound_v3', 'contract': 'Comet'},
    {'decoded_project': 'compound_v3', 'contract': 'cUSDbCv3Comet'},
  ]
%}

{{
  lending_compound_v3_compatible_borrow(
    blockchain = 'base',
    project = 'compound',
    version = '3',
    ctokens = config_ctokens,
    sources = config_sources
  )
}}
