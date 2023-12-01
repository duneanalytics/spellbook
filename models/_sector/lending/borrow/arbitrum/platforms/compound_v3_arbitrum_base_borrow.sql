{{
  config(
    schema = 'compound_v3_arbitrum',
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
    ('USDC.e', 0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA, 0x912ce59144191c1204e64559fe8253a0e49e6548, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8), --ARB
    ('USDC.e', 0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA, 0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8), --WETH
    ('USDC.e', 0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA, 0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8), --GMX
    ('USDC.e', 0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA, 0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8), --WBTC
    ('USDC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x912ce59144191c1204e64559fe8253a0e49e6548, 0xaf88d065e77c8cC2239327C5EDb3A432268e5831), --ARB
    ('USDC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 0xaf88d065e77c8cC2239327C5EDb3A432268e5831), --WETH
    ('USDC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a, 0xaf88d065e77c8cC2239327C5EDb3A432268e5831), --GMX
    ('USDC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f, 0xaf88d065e77c8cC2239327C5EDb3A432268e5831)  --WBTC
  ) as x (asset_symbol, comet_contract_address, collateral_token_address, asset_address)
{% endset %}

{%
  set config_sources = [
    {'decoded_project': 'compound_v3', 'contract': 'Comet'},
    {'decoded_project': 'compound_v3', 'contract': 'cUSDCv3Native'},
  ]
%}

{{
  lending_compound_v3_compatible_borrow(
    blockchain = 'arbitrum',
    project = 'compound',
    version = '3',
    ctokens = config_ctokens,
    sources = config_sources
  )
}}
