{{ config(
    schema = 'nft',
    alias = 'base_trades',
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


{% set nft_models = [
 ref('nft_arbitrum_base_trades')
 ,ref('nft_base_base_trades')
 ,ref('nft_bnb_base_trades')
 ,ref('nft_ethereum_base_trades')
 ,ref('nft_old_base_trades')
 ,ref('nft_optimism_base_trades')
 ,ref('nft_polygon_base_trades')
 ,ref('nft_zksync_base_trades')
 ,ref('nft_scroll_base_trades')
 ,ref('nft_celo_base_trades')
 ,ref('nft_avalanche_c_base_trades')
] %}

with base_union as (
SELECT * FROM  (
{% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        project_version,
        block_date,
        block_month,
        block_time,
        block_number,
        tx_hash,
        project_contract_address,
        trade_category,
        trade_type,
        buyer,
        seller,
        nft_contract_address,
        nft_token_id,
        nft_amount,
        price_raw,
        currency_contract,
        platform_fee_amount_raw,
        royalty_fee_amount_raw,
        platform_fee_address,
        royalty_fee_address,
        sub_tx_trade_id,
        tx_from,
        tx_to,
        tx_data_marker
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
)
select * from base_union
