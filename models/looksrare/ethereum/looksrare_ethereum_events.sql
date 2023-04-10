{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "looksrare",
                                \'["soispoke", "hildobby", "denze"]\') }}'
    )
}}

WITH looksrare_trades AS (
    SELECT *
    , ROW_NUMBER() OVER (PARTITION BY tx_hash, nft_contract_address, token_id ORDER BY evt_index ASC) AS id
    FROM (
        SELECT ta.evt_block_time AS block_time
        , ta.tokenId AS token_id
        , ta.amount AS number_of_items
        , CASE WHEN ta.strategy='0x58d83536d3efedb9f7f2a1ec3bdaad2b1a4dd98c' THEN 'Private Sale' ELSE 'Buy' END AS trade_category
        , ta.maker AS seller
        , ta.taker AS buyer
        , ta.price AS amount_raw
        , ta.currency AS currency_contract
        , ta.collection AS nft_contract_address
        , ta.contract_address AS project_contract_address
        , ta.evt_tx_hash AS tx_hash
        , ta.evt_block_number AS block_number
        , ta.evt_index
        , ta.strategy
        FROM {{ source('looksrare_ethereum','LooksRareExchange_evt_TakerAsk') }} ta
        {% if is_incremental() %}
        WHERE ta.evt_block_time >= date_trunc("day", NOW() - interval '1 week')
        {% endif %}

        UNION ALL

        SELECT tb.evt_block_time AS block_time
        , tb.tokenId AS token_id
        , tb.amount AS number_of_items
        , CASE WHEN tb.strategy='0x58d83536d3efedb9f7f2a1ec3bdaad2b1a4dd98c' THEN 'Private Sale' ELSE 'Offer Accepted' END AS trade_category
        , tb.maker AS seller
        , tb.taker AS buyer
        , tb.price AS amount_raw
        , tb.currency AS currency_contract
        , tb.collection AS nft_contract_address
        , tb.contract_address AS project_contract_address
        , tb.evt_tx_hash AS tx_hash
        , tb.evt_block_number AS block_number
        , tb.evt_index
        , tb.strategy
        FROM {{ source('looksrare_ethereum','LooksRareExchange_evt_TakerBid') }} tb
        {% if is_incremental() %}
        WHERE tb.evt_block_time >= date_trunc("day", NOW() - interval '1 week')
        {% endif %}
        )
    )

, royalties AS (
    SELECT evt_block_time AS block_time
    , evt_tx_hash AS tx_hash
    , evt_index
    , collection AS nft_contract_address
    , tokenId AS token_id
    , amount
    , royaltyRecipient
    , ROW_NUMBER() OVER (PARTITION BY evt_tx_hash, collection, tokenId ORDER BY evt_index ASC) AS id
    FROM {{ source('looksrare_ethereum','LooksRareExchange_evt_RoyaltyPayment') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", NOW() - interval '1 week')
    {% endif %}
    )

, platform_fees AS (
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyStandardSaleForFixedPrice_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyAnyItemFromCollectionForFixedPrice_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyPrivateSale_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyStandardSaleForFixedPriceV1B_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyAnyItemFromCollectionForFixedPriceV1B_call_viewProtocolFee') }}
    )


SELECT 'ethereum' AS blockchain
, 'looksrare' AS project
, 'v1' AS version
, lr.block_time
, date_trunc('day', lr.block_time) AS block_date
, lr.token_id
, tok.name AS collection
, pu.price*lr.amount_raw/POWER(10, pu.decimals) AS amount_usd
, tok.standard as token_standard
, CASE WHEN lr.number_of_items > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
, CAST(lr.number_of_items AS DECIMAL(38,0)) AS number_of_items
, lr.trade_category
, 'Trade' AS evt_type
, CASE WHEN lr.seller=agg.contract_address THEN et.from ELSE lr.seller END AS seller
, CASE WHEN lr.buyer=agg.contract_address THEN et.from ELSE lr.buyer END AS buyer
, lr.amount_raw/POWER(10, pu.decimals) AS amount_original
, CAST(lr.amount_raw AS DECIMAL(38,0)) AS amount_raw
, CASE WHEN lr.currency_contract='0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE pu.symbol END AS currency_symbol
, lr.currency_contract
, lr.nft_contract_address
, lr.project_contract_address
, COALESCE(agg_m.aggregator_name, agg.name) AS aggregator_name
, agg.contract_address AS aggregator_address
, lr.tx_hash
, lr.block_number
, et.from AS tx_from
, et.to AS tx_to
, COALESCE((pf.fee_percentage/100)*lr.amount_raw, 0) AS platform_fee_amount_raw
, COALESCE((pf.fee_percentage/100)*lr.amount_raw/POWER(10, pu.decimals), 0) AS platform_fee_amount
, COALESCE((pf.fee_percentage/100)*pu.price*lr.amount_raw/POWER(10, pu.decimals), 0) platform_fee_amount_usd
, CAST(COALESCE(pf.fee_percentage, 0) AS DOUBLE) AS platform_fee_percentage
, roy.royaltyRecipient AS royalty_fee_receive_address
, CASE WHEN lr.currency_contract='0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE pu.symbol END AS royalty_fee_currency_symbol
, CAST(COALESCE(roy.amount, 0) AS DOUBLE) AS royalty_fee_amount_raw
, COALESCE(roy.amount/POWER(10, pu.decimals), 0) AS royalty_fee_amount
, COALESCE(pu.price*roy.amount/POWER(10, pu.decimals), 0) royalty_fee_amount_usd
, CAST(COALESCE(ROUND(100*roy.amount/lr.amount_raw, 2), 0) AS DOUBLE) AS royalty_fee_percentage
, 'ethereum-looksrare-v1' || lr.tx_hash || lr.nft_contract_address || lr.evt_index AS unique_trade_id
FROM looksrare_trades lr
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain='ethereum'
    AND pu.minute=date_trunc('minute', lr.block_time)
    AND (pu.contract_address=lr.currency_contract
        OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND lr.currency_contract='0x0000000000000000000000000000000000000000'))
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", NOW() - interval '1 week')
    {% endif %}
INNER JOIN {{ source('ethereum','transactions') }} et ON lr.block_time=et.block_time
    AND lr.tx_hash=et.hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", NOW() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON et.to=agg.contract_address
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tok ON lr.nft_contract_address=tok.contract_address
LEFT JOIN royalties roy ON roy.block_time=lr.block_time
    AND roy.tx_hash=lr.tx_hash
    AND roy.nft_contract_address=lr.nft_contract_address
    AND roy.token_id=lr.token_id
    AND roy.id = lr.id
LEFT JOIN platform_fees pf ON pf.contract_address=lr.strategy
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
    ON RIGHT(et.data, agg_m.hash_marker_size) = agg_m.hash_marker
