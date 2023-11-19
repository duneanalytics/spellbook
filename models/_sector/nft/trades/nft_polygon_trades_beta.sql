{{ config(
    schema = 'nft_polygon',
    
    alias = 'trades_beta',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id']
    )
}}
-- (project, project_version, model)
{% set base_models = [
     ('aurem',    'v1',   ref('aurem_polygon_base_trades'))
    ,('dew',    'v1',   ref('dew_polygon_base_trades'))
] %}

WITH cte_prices_patch as (
    SELECT
        contract_address
        ,blockchain
        ,decimals
        ,minute
        ,price
        ,symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain = 'polygon'
    {% if is_incremental() %}
    AND minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    UNION ALL
    SELECT
        {{ var("ETH_ERC20_ADDRESS") }} as contract_address
        ,blockchain
        ,18 as decimals
        ,minute
        ,price
        ,symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain = 'polygon' AND symbol = 'MATIC'
    {% if is_incremental() %}
    AND minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
enriched_trades as (
-- macros/models/sector/nft
{{
    enrich_trades(
        blockchain='polygon',
        models=base_models,
        transactions_model=source('polygon','transactions'),
        tokens_nft_model=ref('tokens_polygon_nft'),
        tokens_erc20_model=ref('tokens_polygon_erc20'),
        prices_model='cte_prices_patch',
        aggregators=ref('nft_polygon_aggregators'),
        aggregator_markers=null
    )
}}
)

select * from enriched_trades
