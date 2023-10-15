{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v1_ethereum',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v1",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc"]\') }}'
    )
}}

-- (blockchain, project, project_version, model, project_start_date)
{% set base_models =
(   'ethereum',
    'uniswap',
    '1',
    ref('uniswap_ethereum_v1_base_trades'),
    '2018-11-01'
) %}


-- macros/models/sector/dex
{{
    dex_enrich_trades(
        models = base_models
        ,transactions_model = source(base_models[0], 'transactions')
        ,tokens_erc20_model = ref('tokens_erc20')
        ,prices_model = source('prices', 'usd')
    )
}}
