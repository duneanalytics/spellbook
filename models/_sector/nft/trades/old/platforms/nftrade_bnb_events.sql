{{ config(
    schema = 'nftrade_bnb',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{%- set project_start_date = '2022-09-17' %}

WITH

-- fill events
source_inventory as (
    SELECT
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        makerAddress as maker_address,
        makerAssetAmount as maker_asset_amount_raw,
        makerAssetAmount/POW(10, 18) as maker_asset_amount,
        bytearray_substring(makerAssetData, 17, 20) as maker_asset_address,
        bytearray_to_uint256(bytearray_substring(makerAssetData, 38, 32)) as maker_id,
        marketplaceIdentifier as marketplace_identifier,
        protocolFeePaid as protocol_fees_raw,
        protocolFeePaid/POW(10, 18) as protocol_fees,
        royaltiesAddress as royalties_address,
        royaltiesAmount as royalty_fees_raw,
        royaltiesAmount/POW(10, 18) as royalty_fees,
        senderAddress as sender_address,
        takerAddress as taker_address,
        takerAssetAmount as taker_asset_amount_raw,
        takerAssetAmount/POW(10, 18) as taker_asset_amount,
        bytearray_substring(takerAssetData, 17, 20) as taker_asset_address,
        bytearray_to_uint256(bytearray_substring(takerAssetData, 38, 32)) as taker_id
    FROM
    {{ source('nftrade_bnb', 'NiftyProtocol_evt_Fill') }}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

source_inventory_enriched as (
    SELECT
        src.*,
        CASE
            WHEN src.maker_id = cast(0 as uint256) OR src.maker_id IS NULL THEN taker_asset_address
            ELSE maker_asset_address
        END as nft_contract_address,
        CASE
            WHEN src.maker_id = cast(0 as uint256) OR src.maker_id IS NULL THEN src.taker_id
            ELSE src.maker_id
        END as token_id,
        CASE
            WHEN src.maker_id = cast(0 as uint256) OR src.maker_id IS NULL THEN src.maker_asset_amount_raw
            ELSE src.taker_asset_amount_raw
        END as amount_raw,
        CASE
            WHEN src.maker_id = cast(0 as uint256) OR src.maker_id IS NULL THEN src.maker_asset_amount
            ELSE src.taker_asset_amount
        END as amount_original,
        CASE
            WHEN src.maker_id = cast(0 as uint256) or src.maker_id IS NULL THEN 'Sell'
            ELSE 'Buy'
        END as trade_category,
        CASE
            WHEN src.maker_id = cast(0 as uint256) or src.maker_id IS NULL THEN src.maker_address
            ELSE src.taker_address
        END as buyer,
        CASE
            WHEN src.maker_id = cast(0 as uint256) or src.maker_id IS NULL THEN src.taker_address
            ELSE src.maker_address
        END as seller,
        CASE
            WHEN src.maker_id = cast(0 as uint256) OR src.maker_id IS NULL THEN (src.protocol_fees/src.maker_asset_amount) * 100
            ELSE (src.protocol_fees/src.taker_asset_amount) * 100
        END as platform_fee_percentage,
        CASE
            WHEN src.maker_id = cast(0 as uint256) OR src.maker_id IS NULL THEN (src.royalty_fees/src.maker_asset_amount) * 100
            ELSE (src.royalty_fees/src.taker_asset_amount) * 100
        END as royalty_fee_percentage
    FROM
    source_inventory src
)

    SELECT
        'bnb' as blockchain,
        'nftrade' as project,
        'v1' as version,
        src.evt_block_time as block_time,
        src.evt_block_number as block_number,
        src.token_id,
        nft_token.name as collection,
        src.amount_raw,
        src.amount_original,
        src.amount_original * p.price as amount_usd,
        CASE
            WHEN erc721.evt_index IS NOT NULL THEN 'erc721'
            ELSE 'erc1155'
        END as token_standard,
        'Single Item Trade' as trade_type,
        CAST(1 AS uint256) AS number_of_items,
        src.trade_category,
        'Trade' as evt_type,
        src.buyer,
        src.seller,
        'BNB' as currency_symbol,
        0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c as currency_contract,
        src.nft_contract_address,
        src.contract_address as project_contract_address,
        agg.name as aggregator_name,
        agg.contract_address as aggregator_address,
        src.evt_tx_hash as tx_hash,
        btx."from" as tx_from,
        btx.to as tx_to,
        CAST(src.protocol_fees_raw AS uint256) as platform_fee_amount_raw,
        CAST(src.protocol_fees AS DOUBLE) as platform_fee_amount,
        CAST(src.protocol_fees * p.price AS DOUBLE) as platform_fee_amount_usd,
        CAST(src.platform_fee_percentage AS DOUBLE) as platform_fee_percentage,
        CAST(src.royalty_fees_raw  AS uint256) as royalty_fee_amount_raw,
        src.royalty_fees as royalty_fee_amount,
        src.royalty_fees * p.price as royalty_fee_amount_usd,
        CAST(src.royalty_fee_percentage AS DOUBLE) as royalty_fee_percentage,
        'BNB' as royalty_fee_currency_symbol,
        royalties_address as royalty_fee_receive_address,
        src.evt_index,
        'bnb-nftrade-v1' || '-' || cast(src.evt_block_number as varchar) || '-' || cast(src.evt_tx_hash as varchar) || '-' ||  cast(src.evt_index as varchar) AS unique_trade_id
    FROM
    source_inventory_enriched src
    INNER JOIN
    {{ source('bnb','transactions') }} btx
        ON btx.block_time = src.evt_block_time
        AND btx.hash = src.evt_tx_hash
        {% if not is_incremental() %}
        AND btx.block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND btx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    LEFT JOIN
    {{ ref('tokens_bnb_nft') }} nft_token
        ON nft_token.contract_address = src.nft_contract_address
    LEFT JOIN
    {{ source('prices','usd') }} p
        ON p.blockchain = 'bnb'
        AND p.minute = date_trunc('minute', src.evt_block_time)
        AND p.contract_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
        {% if not is_incremental() %}
        AND p.minute >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND p.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    LEFT JOIN
    {{ ref('nft_bnb_aggregators') }} agg
        ON agg.contract_address = src.sender_address
    LEFT JOIN
    {{ source('erc721_bnb','evt_transfer') }} erc721
        ON erc721.evt_block_time = src.evt_block_time
        AND erc721.evt_tx_hash = src.evt_tx_hash
        AND erc721.contract_address = src.nft_contract_address
        AND erc721.tokenId = src.token_id
        AND erc721.to = src.buyer
        {% if not is_incremental() %}
        AND erc721.evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND erc721.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}



















