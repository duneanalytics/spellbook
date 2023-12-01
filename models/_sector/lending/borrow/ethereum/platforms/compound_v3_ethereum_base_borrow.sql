{{
  config(
    schema = 'compound_v3_ethereum',
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
    ('WETH', 0xA17581A9E3356d9A858b789D68B4d866e593aE94, 0xBe9895146f7AF43049ca1c1AE358B0541Ea49704, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --cbETH
    ('WETH', 0xA17581A9E3356d9A858b789D68B4d866e593aE94, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --wstETH
    ('WETH', 0xA17581A9E3356d9A858b789D68B4d866e593aE94, 0xae78736Cd615f374D3085123A210448E74Fc6393, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --rETH
    ('USDC', 0xc3d688B66703497DAA19211EEdff47f25384cdc3, 0x514910771AF9Ca656af840dff83E8264EcF986CA, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --LINK
    ('USDC', 0xc3d688B66703497DAA19211EEdff47f25384cdc3, 0xc00e94Cb662C3520282E6f5717214004A7f26888, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --COMP
    ('USDC', 0xc3d688B66703497DAA19211EEdff47f25384cdc3, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --WETH
    ('USDC', 0xc3d688B66703497DAA19211EEdff47f25384cdc3, 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), --UNI
    ('USDC', 0xc3d688B66703497DAA19211EEdff47f25384cdc3, 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599, 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2)  --WBTC
  ) as x (asset_symbol, comet_contract_address, collateral_token_address, asset_address)
{% endset %}

{%
  set config_sources = [
    {'decoded_project': 'compound_v3', 'contract': 'Comet'},
    {'decoded_project': 'compound_v3', 'contract': 'cWETHv3'},
  ]
%}

{{
  lending_compound_v3_compatible_borrow(
    blockchain = 'ethereum',
    project = 'compound',
    version = '3',
    ctokens = config_ctokens,
    sources = config_sources
  )
}}
