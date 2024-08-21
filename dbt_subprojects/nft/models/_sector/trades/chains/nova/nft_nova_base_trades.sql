{{ config(
    schema = 'nft_nova',
    alias = 'base_trades',
    materialized = 'view'
    )
}}


{% set nft_models = [
 ref('king_of_destiny_nova_base_trades')
,ref('opensea_v3_nova_base_trades')
,ref('opensea_v4_nova_base_trades')
] %}

with base_union as (
SELECT * FROM  (
{% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        project_version,
        cast(date_trunc('day', block_time) as date) as block_date,
        cast(date_trunc('month', block_time) as date) as block_month,
        block_time,
        block_number,
        tx_hash,
        project_contract_address,
        trade_category,                 --buy/sell/swap
        trade_type,                     --primary/secondary
        buyer,
        seller,
        nft_contract_address,
        nft_token_id,
        nft_amount,                -- always 1 for erc721
        price_raw,
        currency_contract,
        platform_fee_amount_raw,
        royalty_fee_amount_raw,
        platform_fee_address,   -- optional
        royalty_fee_address,    -- optional
        sub_tx_trade_id,
--        tx_from,
--        tx_to,
        row_number() over (partition by tx_hash, sub_tx_trade_id order by tx_hash) as duplicates_rank   -- duplicates protection
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
where duplicates_rank = 1
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_union', 'nova') }}
