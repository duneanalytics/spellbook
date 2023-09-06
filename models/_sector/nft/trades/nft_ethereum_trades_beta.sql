{{ config(
    schema = 'nft_ethereum',
    tags = ['dunesql'],
    alias = alias('trades_beta'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id']
    )
}}
-- (project, project_version, model)
{% set base_models = [
     ('archipelago',    'v1',   ref('archipelago_ethereum_base_trades'))
    ,('superrare',    'v1',   ref('superrare_ethereum_base_trades'))
    ,('foundation',    'v1',   ref('foundation_ethereum_base_trades'))
    ,('blur',    'v1',   ref('blur_ethereum_base_trades'))
    ,('blur',    'v2',   ref('blur_v2_ethereum_base_trades'))
    ,('element',    'v1',   ref('element_ethereum_base_trades'))
    ,('x2y2',    'v1',   ref('x2y2_ethereum_base_trades'))
    ,('zora',    'v1',   ref('zora_v1_ethereum_base_trades'))
    ,('zora',    'v2',   ref('zora_v2_ethereum_base_trades'))
    ,('zora',    'v3',   ref('zora_v3_ethereum_base_trades'))
    ,('cryptopunks',    'v1',   ref('cryptopunks_ethereum_base_trades'))
    ,('sudoswap',    'v1',   ref('sudoswap_ethereum_base_trades'))
    ,('collectionswap',    'v1',   ref('collectionswap_ethereum_base_trades'))
    ,('looksrare',    'v1',   ref('looksrare_v1_ethereum_base_trades'))
    ,('looksrare',    'v2',   ref('looksrare_v2_ethereum_base_trades'))
    ,('looksrare',    'seaport',   ref('looksrare_seaport_ethereum_base_trades'))
] %}

-- TODO: We should remove this CTE and include ETH into the general prices table once everything is migrated
WITH cte_prices_patch as (
    SELECT
        contract_address
        ,blockchain
        ,decimals
        ,minute
        ,price
        ,symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain = 'ethereum'
    {% if is_incremental() %}
    AND minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    UNION ALL
    SELECT
        {{ var("ETH_ERC20_ADDRESS") }} as contract_address
        ,'ethereum' as blockchain
        ,18 as decimals
        ,minute
        ,price
        ,'ETH' as symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain is null AND symbol = 'ETH'
    {% if is_incremental() %}
    AND minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
enriched_trades as (
-- macros/models/sector/nft
{{
    enrich_trades(
        blockchain='ethereum',
        models=base_models,
        transactions_model=source('ethereum','transactions'),
        tokens_nft_model=ref('tokens_ethereum_nft'),
        tokens_erc20_model=ref('tokens_ethereum_erc20'),
        prices_model='cte_prices_patch',
        aggregators=ref('nft_ethereum_aggregators'),
        aggregator_markers=ref('nft_ethereum_aggregators_markers')
    )
}}
)

select * from enriched_trades
