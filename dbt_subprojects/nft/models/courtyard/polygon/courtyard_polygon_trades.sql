{{ config(
    schema = 'courtyard_polygon',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                            "project",
                            "courtyard",
                            \'["mukhtaryah"]\') }}'
    )
}}

-- Courtyard trades on Polygon via Seaport
SELECT 
    s.blockchain,
    'courtyard' as project,
    s.version,
    s.block_date,
    s.block_month,
    s.block_time,
    s.block_number,
    s.tx_hash,
    s.evt_index,
    s.buyer,
    s.seller,
    s.price as price_usd,  -- Changed from s.price_usd
    s.platform_fee_amount as platform_fee_usd,  -- Changed from s.platform_fee_usd
    s.royalty_fee_amount as royalty_fee_usd,  -- Changed from s.royalty_fee_usd
    s.nft_contract_address,
    s.token_id,
    s.token_standard,
    s.token_amount,
    s.trade_type,
    s.payment_token,
    s.price,
    s.sub_type,
    s.sub_idx
FROM {{ ref('seaport_polygon_trades') }} s
WHERE s.nft_contract_address = 0x251be3a17af4892035c37ebf5890f4a4d889dcad
{% if is_incremental() %}
AND s.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}