{{ config(
    schema = 'nft_ethereum',
    alias = 'base_trades',
    materialized = 'view'
    )
}}


{% set nft_models = [
 ref('archipelago_ethereum_base_trades')
,ref('blur_ethereum_base_trades')
,ref('blur_seaport_ethereum_base_trades')
,ref('blur_v2_ethereum_base_trades')
,ref('collectionswap_ethereum_base_trades')
,ref('cryptopunks_ethereum_base_trades')
,ref('element_ethereum_base_trades')
,ref('foundation_ethereum_base_trades')
,ref('liquidifty_ethereum_base_trades')
,ref('looksrare_seaport_ethereum_base_trades')
,ref('looksrare_v1_ethereum_base_trades')
,ref('looksrare_v2_ethereum_base_trades')
,ref('sudoswap_ethereum_base_trades')
,ref('superrare_ethereum_base_trades')
,ref('trove_ethereum_base_trades')
,ref('x2y2_ethereum_base_trades')
,ref('zora_v1_ethereum_base_trades')
,ref('zora_v2_ethereum_base_trades')
,ref('zora_v3_ethereum_base_trades')
,ref('magiceden_ethereum_base_trades')
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
--        tx_from,              -- not yet available in the base event tables
--        tx_to,                -- not yet available in the base event tables
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
{{ add_nft_tx_data('base_union', 'ethereum') }}
