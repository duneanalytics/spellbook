{{ config(
    schema = 'nftrade_bnb',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
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
            WHEN src.maker_id = uint256 '0' OR src.maker_id IS NULL THEN taker_asset_address
            ELSE maker_asset_address
        END as nft_contract_address,
        CASE
            WHEN src.maker_id = uint256 '0' OR src.maker_id IS NULL THEN src.taker_id
            ELSE src.maker_id
        END as token_id,
        CASE
            WHEN src.maker_id = uint256 '0' OR src.maker_id IS NULL THEN src.maker_asset_amount_raw
            ELSE src.taker_asset_amount_raw
        END as amount_raw,
        CASE
            WHEN src.maker_id = uint256 '0' OR src.maker_id IS NULL THEN src.maker_asset_amount
            ELSE src.taker_asset_amount
        END as amount_original,
        CASE
            WHEN src.maker_id = uint256 '0' or src.maker_id IS NULL THEN 'Sell'
            ELSE 'Buy'
        END as trade_category,
        CASE
            WHEN src.maker_id = uint256 '0' or src.maker_id IS NULL THEN src.maker_address
            ELSE src.taker_address
        END as buyer,
        CASE
            WHEN src.maker_id = uint256 '0' or src.maker_id IS NULL THEN src.taker_address
            ELSE src.maker_address
        END as seller,
        CASE
            WHEN src.maker_id = uint256 '0' OR src.maker_id IS NULL THEN (src.protocol_fees/src.maker_asset_amount) * 100
            ELSE (src.protocol_fees/src.taker_asset_amount) * 100
        END as platform_fee_percentage,
        CASE
            WHEN src.maker_id = uint256 '0' OR src.maker_id IS NULL THEN (src.royalty_fees/src.maker_asset_amount) * 100
            ELSE (src.royalty_fees/src.taker_asset_amount) * 100
        END as royalty_fee_percentage
    FROM
    source_inventory src
)

, base_trades as (
    SELECT
        'bnb' as blockchain,
        'nftrade' as project,
        'v1' as project_version,
        src.evt_block_time as block_time,
        cast(date_trunc('day', src.evt_block_time) as date) as block_date,
        cast(date_trunc('month', src.evt_block_time) as date) as block_month,
        src.evt_block_number as block_number,
        src.token_id as nft_token_id,
        src.amount_raw as price_raw,
        'secondary' as trade_type,
        uint256 '1' AS nft_amount,
        src.trade_category,
        src.buyer,
        src.seller,
        0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c as currency_contract,
        src.nft_contract_address,
        src.contract_address as project_contract_address,
        src.evt_tx_hash as tx_hash,
        CAST(src.protocol_fees_raw AS uint256) as platform_fee_amount_raw,
        CAST(src.royalty_fees_raw  AS uint256) as royalty_fee_amount_raw,
        royalties_address as royalty_fee_address,
        cast(null as varbinary) as platform_fee_address,
        src.evt_index as sub_tx_trade_id
    FROM source_inventory_enriched src

)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'bnb') }}

















