{{ config(
    schema = 'decentraland_polygon',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
)}}

{% set nft_start_date = '2022-01-04' %}

WITH

src_data_enriched as (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        assetId,
        buyer,
        nftAddress,
        seller,
        totalPrice/POWER(10, 18) as price_converted,
        totalPrice as price_raw
        -- '0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4' as token_address
    FROM
    {{ source('decentraland_polygon','MarketplaceV2_evt_OrderSuccessful') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT
    'polygon' as blockchain,
    'decentraland' as project,
    'v1' as version,
    date_trunc('day', src.evt_block_time) AS block_date,
    src.evt_block_time AS block_time,
    src.evt_block_number AS block_number,
    src.price_converted * p.price as amount_usd,
    src.price_converted as amount_original,
    src.price_raw as amount_raw,
    'MANA' as currency_symbol,
    0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4 as currency_contract,
    src.assetId as token_id,
    'erc721' as token_standard,
    src.contract_address as project_contract_address,
    'Trade' AS evt_type,
    CAST(NULL AS varchar) AS collection,
    'Single Item Trade' as trade_type,
    uint256 '1' as number_of_items,
    'buy' AS trade_category,
    src.buyer,
    src.seller,
    src.nftAddress as nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    src.evt_tx_hash as tx_hash,
    t."from" AS tx_from,
    t."to" AS tx_to,
    CAST(COALESCE(src.price_raw, uint256 '0') * 0.025 as uint256) as platform_fee_amount_raw,
    CAST(COALESCE(src.price_converted, 0) * 0.025 as double) as platform_fee_amount,
    CAST(COALESCE(src.price_converted, 0) * p.price * 0.025 as double) as platform_fee_amount_usd,
    CAST(2.5 as double) as platform_fee_percentage,
    CAST(NULL as uint256) as royalty_fee_amount_raw,
    CAST(NULL as double) as royalty_fee_amount,
    CAST(NULL as double) as royalty_fee_amount_usd,
    CAST(NULL as double) as royalty_fee_percentage,
    CAST(NULL as varbinary) as royalty_fee_receive_address,
    CAST(NULL AS varchar) AS royalty_fee_currency_symbol,
    cast(src.evt_tx_hash as varchar) || '-' || 'Trade' || '-' || cast(src.evt_index as varchar)|| '-' || cast(src.assetId as varchar) AS unique_trade_id
FROM src_data_enriched src
INNER JOIN
{{ source('polygon','transactions') }} t
    ON src.evt_block_number = t.block_number
    AND src.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= TIMESTAMP '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN
{{ source('prices', 'usd') }} p
    ON p.blockchain = 'polygon'
    AND p.contract_address = 0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4
    AND p.minute = date_trunc('minute', src.evt_block_time)
    {% if not is_incremental() %}
    AND p.minute >= TIMESTAMP '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN
{{ ref('nft_aggregators') }} agg
    ON agg.blockchain = 'polygon'
    AND agg.contract_address = t.to
