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
                                \'["soispoke", "hildobby"]\') }}'
    )
}}

WITH looksrare_trades AS (
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
    FROM {{ source('looksrare_ethereum','looksrareexchange_evt_takerask') }} ta
    {% if is_incremental() %}
    WHERE ta.evt_block_time >= date_trunc("day", now() - interval '1 week')
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
    FROM {{ source('looksrare_ethereum','looksrareexchange_evt_takerbid') }} tb
    {% if is_incremental() %}
    WHERE tb.evt_block_time >= date_trunc("day", now() - interval '1 week')
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

SELECT distinct 'ethereum' AS blockchain
, 'looksrare' AS project
, 'v1' AS version
, lr.block_time
, date_trunc('day', lr.block_time) AS block_date
, lr.token_id
, tok.name AS collection
, pu.price*lr.amount_raw/POWER(10, pu.decimals) AS amount_usd
, CASE WHEN standard.evt_index IS NULL THEN 'erc1155' ELSE 'erc721' END AS token_standard
, CASE WHEN lr.number_of_items > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
, CAST(lr.number_of_items AS DECIMAL(38,0)) AS number_of_items
, lr.trade_category
, 'Trade' AS evt_type
, COALESCE(seller_fix.from, lr.seller) AS seller
, COALESCE(buyer_fix.to, lr.buyer) AS buyer
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
, 'ethereum-looksrare-v1' || COALESCE(lr.tx_hash, '-1') || COALESCE(lr.nft_contract_address, '-1') || COALESCE(lr.token_id, '-1') || COALESCE(COALESCE(seller_fix.from, lr.seller), '-1') || COALESCE(COALESCE(buyer_fix.to, lr.buyer), '-1') || COALESCE(lr.evt_index, '-1') AS unique_trade_id
FROM looksrare_trades lr
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain='ethereum'
    AND pu.minute=date_trunc('minute', lr.block_time)
    AND (pu.contract_address=lr.currency_contract
        OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND lr.currency_contract='0x0000000000000000000000000000000000000000'))
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
INNER JOIN {{ source('ethereum','transactions') }} et ON lr.block_time=et.block_time
    AND lr.tx_hash=et.hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON et.to=agg.contract_address
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tok ON lr.nft_contract_address=tok.contract_address
LEFT JOIN {{ source('looksrare_ethereum','looksrareexchange_evt_royaltypayment') }} roy ON roy.evt_block_time=lr.block_time
    AND roy.evt_tx_hash=lr.tx_hash
    AND roy.collection=lr.nft_contract_address
    AND roy.tokenId=lr.token_id
    {% if is_incremental() %}
    AND roy.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT ANTI JOIN {{ source('erc20_ethereum', 'evt_transfer') }} anti_roy ON anti_roy.evt_block_time=lr.block_time
    AND anti_roy.evt_tx_hash=lr.tx_hash
    AND anti_roy.contract_address=roy.currency
    AND anti_roy.contract_address=roy.currency
    AND anti_roy.to=roy.royaltyRecipient
    AND anti_roy.from!=lr.buyer
    {% if is_incremental() %}
    AND anti_roy.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} buyer_fix ON lr.block_time=buyer_fix.block_time
    AND lr.tx_hash=buyer_fix.tx_hash
    AND lr.nft_contract_address=buyer_fix.contract_address
    AND lr.token_id=buyer_fix.token_id
    AND lr.buyer=agg.contract_address
    AND lr.buyer=buyer_fix.from
    {% if is_incremental() %}
    AND buyer_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} seller_fix ON lr.block_time=seller_fix.block_time
    AND lr.tx_hash=seller_fix.tx_hash
    AND lr.nft_contract_address=seller_fix.contract_address
    AND lr.token_id=seller_fix.token_id
    AND lr.seller=agg.contract_address
    AND lr.seller=seller_fix.to
    {% if is_incremental() %}
    AND seller_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} standard ON lr.block_time=standard.evt_block_time
    AND lr.tx_hash=standard.evt_tx_hash
    AND lr.nft_contract_address=standard.contract_address
    AND lr.token_id=standard.tokenId
    AND COALESCE(seller_fix.from, lr.seller)=standard.from
    {% if is_incremental() %}
    AND standard.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN platform_fees pf ON pf.contract_address=lr.strategy
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
    ON LEFT(et.data, CHARINDEX(agg_m.hash_marker, et.data) + LENGTH(agg_m.hash_marker)) LIKE '%' || agg_m.hash_marker