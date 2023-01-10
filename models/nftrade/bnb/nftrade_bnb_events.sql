{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "nftrade",
                                \'["Henrystats"]\') }}'
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
        makerAddress,
        makerAssetAmount as makerAssetAmountRaw,
        makerAssetAmount/POW(10, 18) as makerAssetAmount,
        CONCAT('0x', SUBSTRING(makerAssetData, 35, 40)) as makerAssetAddress,
        CAST(bytea2numeric_v2(SUBSTRING(makerAssetData, 77, 64)) as decimal(38)) as makerId,
        marketplaceIdentifier,
        protocolFeePaid * 1 as protocol_fees_raw,
        protocolFeePaid/POW(10, 18) as protocol_fees,
        royaltiesAddress,
        royaltiesAmount as royalty_fees_raw,
        royaltiesAmount/POW(10, 18) as royalty_fees,
        senderAddress,
        takerAddress,
        takerAssetAmount as takerAssetAmountRaw,
        takerAssetAmount/POW(10, 18) as takerAssetAmount,
        CONCAT('0x', SUBSTRING(takerAssetData, 35, 40)) as takerAssetAddress,
        CAST(bytea2numeric_v2(SUBSTRING(takerAssetData, 77, 64)) as decimal(38)) as takerId
    FROM
    {{ source('nftrade_bnb', 'NiftyProtocol_evt_Fill') }}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

source_inventory_enriched as (
    SELECT
        src.*,
        CASE
            WHEN src.makerId = 0 OR src.makerId IS NULL THEN takerAssetAddress
            ELSE makerAssetAddress
        END as nft_contract_address,
        CAST((CASE
            WHEN src.makerId = 0 OR src.makerId IS NULL THEN src.takerId
            ELSE src.makerId
        END) AS VARCHAR(100)) as token_id,
        CAST((CASE
            WHEN src.makerId = 0 OR src.makerId IS NULL THEN src.makerAssetAmountRaw
            ELSE src.takerAssetAmountRaw
        END) AS DECIMAL(38, 0)) as amount_raw,
        CASE
            WHEN src.makerId = 0 OR src.makerId IS NULL THEN src.makerAssetAmount
            ELSE src.takerAssetAmount
        END as amount_original,
        CASE
            WHEN src.makerId = 0 or src.makerId IS NULL THEN 'Sell'
            ELSE 'buy'
        END as trade_category,
        CASE
            WHEN src.makerId = 0 or src.makerId IS NULL THEN src.makerAddress
            ELSE src.takerAddress
        END as buyer,
        CASE
            WHEN src.makerId = 0 or src.makerId IS NULL THEN src.takerAddress
            ELSE src.makerAddress
        END as seller,
        CASE
            WHEN src.makerId = 0 OR src.makerId IS NULL THEN (src.protocol_fees/src.makerAssetAmount) * 100
            ELSE (src.protocol_fees/src.takerAssetAmount) * 100
        END as platform_fee_percentage,
        CASE
            WHEN src.makerId = 0 OR src.makerId IS NULL THEN (src.royalty_fees/src.makerAssetAmount) * 100
            ELSE (src.royalty_fees/src.takerAssetAmount) * 100
        END as royalty_fee_percentage
    FROM
    source_inventory src
)

    SELECT
        'bnb' as blockchain,
        'nftrade' as project,
        'v1' as version,
        src.evt_block_time as block_time,
        date_trunc('day', src.evt_block_time) as block_date,
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
        CAST(1 AS DECIMAL(38,0)) AS number_of_items,
        src.trade_category,
        'Trade' as evt_type,
        src.buyer,
        src.seller,
        'BNB' as currency_symbol,
        LOWER('0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c') as currency_contract,
        src.nft_contract_address,
        src.contract_address as project_contract_address,
        agg.name as aggregator_name,
        agg.contract_address as aggregator_address,
        src.evt_tx_hash as tx_hash,
        btx.from as tx_from,
        btx.to as tx_to,
        src.protocol_fees_raw as platform_fee_amount_raw,
        src.protocol_fees as platform_fee_amount,
        src.protocol_fees * p.price as platform_fee_amount_usd,
        src.platform_fee_percentage,
        src.royalty_fees_raw as royalty_fee_amount_raw,
        src.royalty_fees as royalty_fee_amount,
        src.royalty_fees * p.price as royalty_fee_amount_usd,
        src.royalty_fee_percentage,
        'BNB' as royalty_fee_currency_symbol,
        royaltiesAddress as royalty_fee_receive_address,
        'bnb-nftrade-v1' || '-' || src.evt_block_number || '-' || src.evt_tx_hash || '-' ||  src.evt_index AS unique_trade_id
    FROM
    source_inventory_enriched src
    INNER JOIN
    {{ source('bnb','transactions') }} btx
        ON btx.block_time = src.evt_block_time
        AND btx.hash = src.evt_tx_hash
        {% if not is_incremental() %}
        AND btx.block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND btx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN
    {{ ref('tokens_bnb_nft') }} nft_token
        ON nft_token.contract_address = src.nft_contract_address
    LEFT JOIN
    {{ source('prices','usd') }} p
        ON p.blockchain = 'bnb'
        AND p.minute = date_trunc('minute', src.evt_block_time)
        AND p.contract_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
        {% if not is_incremental() %}
        AND p.minute >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND p.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN
    {{ ref('nft_bnb_aggregators') }} agg
        ON agg.contract_address = src.senderAddress
    LEFT JOIN
    {{ source('erc721_ethereum','evt_transfer') }} erc721
        ON erc721.evt_block_time = src.evt_block_time
        AND erc721.evt_tx_hash = src.evt_tx_hash
        AND erc721.contract_address = src.nft_contract_address
        AND erc721.tokenId = src.token_id
        AND erc721.to = src.buyer
        {% if not is_incremental() %}
        AND erc721.evt_block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND erc721.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}



















