{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "x2y2",
                                \'["hildobby","soispoke"]\') }}'
    )
}}

SELECT distinct 'ethereum' AS blockchain
, 'x2y2' AS project
, 'v1' AS version
, prof.evt_block_time AS block_time
, date_trunc('day', prof.evt_block_time) AS block_date
, prof.evt_block_number AS block_number
, COALESCE(bytea2numeric_v2(substring(get_json_object(inv.item, '$.data'), 195,64))::BIGINT, bytea2numeric_v2(substring(get_json_object(inv.item, '$.data'), 195,64))) AS token_id
, nft_token.name AS collection
, prof.amount AS amount_raw
, prof.amount/POWER(10, currency_token.decimals) AS amount_original
, pu.price*prof.amount/POWER(10, currency_token.decimals) AS amount_usd
, CASE WHEN get_json_object(inv.detail, '$.executionDelegate')='0xf849de01b080adc3a814fabe1e2087475cf2e354' THEN 'erc721'
    WHEN get_json_object(inv.detail, '$.executionDelegate')='0x024ac22acdb367a3ae52a3d94ac6649fdc1f0779' THEN 'erc1155'
    END AS token_standard
, 'Single Item Trade' AS trade_type
, 1 AS number_of_items
, CASE WHEN et.from=COALESCE(MAX(seller_fix.from), inv.maker) THEN 'Offer Accepted'
    ELSE 'Buy' -- AND ADD PRIVATE ONES
    END AS trade_category
, 'Trade' AS evt_type
, COALESCE(MAX(buyer_fix.to), inv.taker) AS buyer
, COALESCE(MAX(seller_fix.from), inv.maker) AS seller
, CASE WHEN prof.currency='0x0000000000000000000000000000000000000000' THEN 'ETH'
    ELSE currency_token.symbol
    END AS currency_symbol
, CASE WHEN prof.currency='0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE prof.currency
    END AS currency_contract
, '0x' || substring(get_json_object(inv.item, '$.data'), 155, 40) AS nft_contract_address
, prof.contract_address AS project_contract_address
, COALESCE(agg_m.aggregator_name, agg.name) AS aggregator_name
, agg.contract_address AS aggregator_address
, prof.evt_tx_hash AS tx_hash
, et.from AS tx_from
, et.to AS tx_to
, ROUND(COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0), 0) AS platform_fee_amount_raw
, ROUND(COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0), 0)/POWER(10, currency_token.decimals) AS platform_fee_amount
, pu.price*ROUND(COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6), 0)/POWER(10, currency_token.decimals) AS platform_fee_amount_usd
, COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0) AS platform_fee_percentage
, COALESCE(get_json_object(inv.item, '$.price')*SUM(COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[4]'), '$.percentage'), 0))/1e6, 0) AS royalty_fee_amount_raw
, COALESCE(get_json_object(inv.item, '$.price')*SUM(COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[4]'), '$.percentage'), 0))/1e6, 0)/POWER(10, currency_token.decimals) AS royalty_fee_amount
, pu.price*COALESCE(get_json_object(inv.item, '$.price')*SUM(COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[4]'), '$.percentage'), 0))/1e6, 0)/POWER(10, currency_token.decimals) royalty_fee_amount_usd
, SUM(COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)+COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[4]'), '$.percentage'), 0))/1e6 AS royalty_fee_percentage
, get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.to') AS royalty_fee_receive_address
, 'ethereum-x2y2-v1' || COALESCE(prof.evt_tx_hash, '-1') || COALESCE(inv.taker, '-1') || COALESCE(inv.maker, '-1') || COALESCE('0x' || substring(get_json_object(inv.item, '$.data'), 155, 40), '-1') || COALESCE(bytea2numeric_v2(substring(get_json_object(inv.item, '$.data'), 195,64))::BIGINT, bytea2numeric_v2(substring(get_json_object(inv.item, '$.data'), 195,64))) AS unique_trade_id
FROM {{ source('x2y2_ethereum','X2Y2_r1_evt_EvProfit') }} prof
INNER JOIN {{ source('x2y2_ethereum','X2Y2_r1_evt_EvInventory') }} inv  ON inv.evt_block_time=prof.evt_block_time
    AND inv.itemHash = prof.itemHash
    {% if is_incremental() %}
    AND inv.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
INNER JOIN {{ source('ethereum','transactions') }} et ON et.block_time=prof.evt_block_time
    AND et.hash=prof.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_token ON ('0x' || substring(get_json_object(inv.item, '$.data'), 155, 40)) = nft_token.contract_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} currency_token ON currency_token.contract_address=prof.currency
        OR (currency_token.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND prof.currency='0x0000000000000000000000000000000000000000')
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain='ethereum'
    AND pu.minute=date_trunc('minute', prof.evt_block_time)
    AND (pu.contract_address=prof.currency
        OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND prof.currency='0x0000000000000000000000000000000000000000'))
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} buyer_fix ON prof.evt_block_time=buyer_fix.block_time
    AND prof.evt_tx_hash=buyer_fix.tx_hash
    AND '0x' || substring(get_json_object(inv.item, '$.data'), 155, 40)=buyer_fix.contract_address
    AND inv.taker=agg.contract_address
    AND inv.taker=buyer_fix.from
    {% if is_incremental() %}
    AND buyer_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} seller_fix ON prof.evt_block_time=seller_fix.block_time
    AND prof.evt_tx_hash=seller_fix.tx_hash
    AND '0x' || substring(get_json_object(inv.item, '$.data'), 155, 40)=seller_fix.contract_address
    AND inv.maker=agg.contract_address
    AND inv.maker=seller_fix.to
    {% if is_incremental() %}
    AND seller_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
        ON LEFT(et.data, CHARINDEX(agg_m.hash_marker, et.data) + LENGTH(agg_m.hash_marker)) LIKE '%' || agg_m.hash_marker
{% if is_incremental() %}
WHERE prof.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
GROUP BY prof.evt_block_time, prof.evt_block_number, inv.item, nft_token.name, prof.amount, currency_token.decimals
, inv.maker, inv.taker, prof.currency, currency_token.symbol, prof.contract_address
, agg_m.aggregator_name, agg.name, agg.contract_address, prof.evt_tx_hash, et.from, et.to, inv.detail, pu.price