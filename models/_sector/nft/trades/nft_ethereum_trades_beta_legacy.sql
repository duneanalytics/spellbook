{{ config(
    schema = 'nft_ethereum',
    alias = alias('trades_beta',legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id']
    )
}}
-- (project, project_version, model)
{% set base_models = [
     ('archipelago',    'v1',   ref('archipelago_ethereum_base_trades_legacy'))
    ,('superrare',    'v1',   ref('superrare_ethereum_base_trades_legacy'))
    ,('foundation',    'v1',   ref('foundation_ethereum_base_trades_legacy'))
    ,('blur',    'v1',   ref('blur_ethereum_base_trades_legacy'))
    ,('element',    'v1',   ref('element_ethereum_base_trades_legacy'))
    ,('x2y2',    'v1',   ref('x2y2_ethereum_base_trades_legacy'))
    ,('zora',    'v1',   ref('zora_v1_ethereum_base_trades_legacy'))
    ,('zora',    'v2',   ref('zora_v2_ethereum_base_trades_legacy'))
    ,('zora',    'v3',   ref('zora_v3_ethereum_base_trades_legacy'))
    ,('cryptopunks',    'v1',   ref('cryptopunks_ethereum_base_trades_legacy'))
    ,('sudoswap',    'v1',   ref('sudoswap_ethereum_base_trades_legacy'))
    ,('collectionswap',    'v1',   ref('collectionswap_ethereum_base_trades_legacy'))
    ,('looksrare',    'v1',   ref('looksrare_v1_ethereum_base_trades_legacy'))
    ,('looksrare',    'v2',   ref('looksrare_v2_ethereum_base_trades_legacy'))
] %}

-- We should remove this CTE and include ETH into the general prices table once everything is migrated
WITH cte_prices_patch as (
    SELECT
        contract_address
        ,blockchain
        ,decimals
        ,minute
        ,price
        ,symbol
    FROM {{ ref('prices_usd_forward_fill_legacy') }}
    WHERE blockchain = 'ethereum'
    {% if is_incremental() %}
    AND minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    SELECT
        '{{ var("ETH_ERC20_ADDRESS") }}' as contract_address
        ,'ethereum' as blockchain
        ,18 as decimals
        ,minute
        ,price
        ,'ETH' as symbol
    FROM {{ ref('prices_usd_forward_fill_legacy') }}
    WHERE blockchain is null AND symbol = 'ETH'
    {% if is_incremental() %}
    AND minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
enriched_trades as (
-- macros/models/sector/nft
{{
    enrich_trades_legacy(
        blockchain='ethereum',
        models=base_models,
        transactions_model=source('ethereum','transactions'),
        tokens_nft_model=ref('tokens_ethereum_nft_legacy'),
        tokens_erc20_model=ref('tokens_ethereum_erc20_legacy'),
        prices_model='cte_prices_patch',
        aggregators=ref('nft_ethereum_aggregators_legacy'),
        aggregator_markers=ref('nft_ethereum_aggregators_markers_legacy')
    )
}}
)

select * from enriched_trades
