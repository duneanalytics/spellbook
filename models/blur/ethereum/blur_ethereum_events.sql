{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "blur",
                                \'["hildobby","pandajackson42"]\') }}')
}}

{% set project_start_date = '2022-10-18' %}
{% set seaport_usage_start_date = '2023-01-25' %}


SELECT
    'ethereum' AS blockchain
    , 'blur' AS project
    , 'v1' AS version
    , CAST(date_trunc('day', bm.evt_block_time) AS timestamp) AS block_date
    , CAST(bm.evt_block_time AS timestamp) AS block_time
    , CAST(bm.evt_block_number AS double) AS block_number
    , CAST(get_json_object(bm.sell, '$.tokenId') AS string) AS token_id
    , nft.standard AS token_standard
    , nft.name AS collection
    , CASE WHEN get_json_object(bm.buy, '$.amount')=1 THEN 'Single Item Trade'
        ELSE 'Bundle Trade'
        END AS trade_type
    , CAST(get_json_object(bm.buy, '$.amount') AS DECIMAL(38,0)) AS number_of_items
    , 'Trade' AS evt_type
    , CASE WHEN get_json_object(bm.sell, '$.trader') = agg.contract_address THEN et.from ELSE get_json_object(bm.sell, '$.trader') END AS seller
    ,CASE WHEN get_json_object(bm.buy, '$.trader') = agg.contract_address THEN et.from ELSE get_json_object(bm.buy, '$.trader') END AS buyer
    , CASE WHEN get_json_object(bm.buy, '$.matchingPolicy') IN ('0x00000000006411739da1c40b106f8511de5d1fac', '0x0000000000dab4a563819e8fd93dba3b25bc3495') THEN 'Buy'
        WHEN get_json_object(bm.buy, '$.matchingPolicy') IN ('0x0000000000b92d5d043faf7cecf7e2ee6aaed232') THEN 'Offer Accepted'
        WHEN et.from=get_json_object(bm.buy, '$.trader') THEN 'Buy'
        WHEN et.from=get_json_object(bm.sell, '$.trader') THEN 'Offer Accepted'
        ELSE 'Unknown'
        END AS trade_category
    , CAST(get_json_object(bm.buy, '$.price') AS DECIMAL(38,0)) AS amount_raw
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN CAST(get_json_object(bm.buy, '$.price')/POWER(10, 18) AS double)
        ELSE CAST(get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals) AS double)
        END AS amount_original
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN CAST(pu.price*get_json_object(bm.buy, '$.price')/POWER(10, 18) AS double)
        ELSE CAST(pu.price*get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals) AS double)
        END AS amount_usd
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN 'ETH'
        ELSE pu.symbol
        END AS currency_symbol
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE get_json_object(bm.buy, '$.paymentToken')
        END AS currency_contract
    , bm.contract_address AS project_contract_address
    , get_json_object(bm.buy, '$.collection') AS nft_contract_address
    , coalesce(agg.name,agg_m.aggregator_name) AS aggregator_name
    , agg.contract_address AS aggregator_address
    , bm.evt_tx_hash AS tx_hash
    , et.from AS tx_from
    , et.to AS tx_to
    , CAST(0 AS DOUBLE) AS platform_fee_amount_raw
    , CAST(0 AS DOUBLE) AS platform_fee_amount
    , CAST(0 AS DOUBLE) AS platform_fee_amount_usd
    , CAST(0 AS DOUBLE) AS platform_fee_percentage
    , CAST(COALESCE(get_json_object(bm.buy, '$.price')*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS DOUBLE) AS royalty_fee_amount_raw
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN CAST(COALESCE(get_json_object(bm.buy, '$.price')/POWER(10, 18)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS DOUBLE)
        ELSE CAST(COALESCE(get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS DOUBLE)
        END AS royalty_fee_amount
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN CAST(COALESCE(pu.price*get_json_object(bm.buy, '$.price')/POWER(10, 18)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS DOUBLE)
        ELSE CAST(COALESCE(pu.price*get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS DOUBLE)
        END AS royalty_fee_amount_usd
    , CAST(COALESCE(get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/100, 0) AS DOUBLE) AS royalty_fee_percentage
    , get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.recipient') AS royalty_fee_receive_address
    , CASE WHEN get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.recipient') IS NOT NULL AND get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN 'ETH'
        WHEN get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.recipient') IS NOT NULL THEN pu.symbol
        END AS royalty_fee_currency_symbol
    ,  CAST('ethereum-blur-v1-' || bm.evt_block_number || '-' || bm.evt_tx_hash || '-' || bm.evt_index AS string) AS unique_trade_id
FROM {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }} bm
JOIN {{ source('ethereum','transactions') }} et ON et.block_number=bm.evt_block_number
    AND et.hash=bm.evt_tx_hash
    {% if not is_incremental() %}
    AND et.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
    ON RIGHT(et.data, agg_m.hash_marker_size) = agg_m.hash_marker
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain='ethereum'
    AND pu.minute=date_trunc('minute', bm.evt_block_time)
    AND (pu.contract_address=get_json_object(bm.buy, '$.paymentToken')
        OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND get_json_object(bm.buy, '$.paymentToken')='0x0000000000000000000000000000000000000000')
        OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND get_json_object(bm.buy, '$.paymentToken')='0x0000000000a39bb272e79075ade125fd351887ac'))
    {% if not is_incremental() %}
    AND pu.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft ON get_json_object(bm.buy, '$.collection')=nft.contract_address
{% if is_incremental() %}
WHERE bm.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL


SELECT
    'ethereum' AS blockchain
    , 'blur' AS project
    , 'v1' AS version
    , CAST(date_trunc('day', s.evt_block_time) AS timestamp) AS block_date
    , CAST(s.evt_block_time AS timestamp) AS block_time
    , CAST(s.evt_block_number AS double) AS block_number
    , CAST(get_json_object(s.offer[0], '$.identifier') AS string) AS token_id
    , nft_tok.standard AS token_standard
    , nft_tok.name AS collection
    , CASE WHEN get_json_object(s.offer[0], '$.amount')=1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type
    , CAST(get_json_object(s.offer[0], '$.amount') AS DECIMAL(38,0)) AS number_of_items
    , 'Trade' AS evt_type
    , s.offerer AS seller
    , s.recipient AS buyer
    , 'Buy' AS trade_category
    , CAST(get_json_object(s.consideration[0], '$.amount')+get_json_object(s.consideration[1], '$.amount') AS DECIMAL(38,0)) AS amount_raw
    , CAST((get_json_object(s.consideration[0], '$.amount')+get_json_object(s.consideration[1], '$.amount'))/POWER(10, 18) AS double) AS amount_original
    , CAST(pu.price*(get_json_object(s.consideration[0], '$.amount')+get_json_object(s.consideration[1], '$.amount'))/POWER(10, 18) AS double) AS amount_usd
    , CASE WHEN get_json_object(s.consideration[0], '$.token')='0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE currency_tok.symbol END AS currency_symbol
    , get_json_object(s.consideration[0], '$.token') AS currency_contract
    , s.contract_address AS project_contract_address
    , get_json_object(s.offer[0], '$.token') AS nft_contract_address
    , CAST(NULL AS string) AS aggregator_name
    , CAST(NULL AS string) AS aggregator_address
    , s.evt_tx_hash AS tx_hash
    , tx.from AS tx_from
    , tx.to AS tx_to
    , CAST(0 AS DOUBLE) AS platform_fee_amount_raw
    , CAST(0 AS DOUBLE) AS platform_fee_amount
    , CAST(0 AS DOUBLE) AS platform_fee_amount_usd
    , CAST(0 AS DOUBLE) AS platform_fee_percentage
    , LEAST(CAST(get_json_object(s.consideration[0], '$.amount') AS DOUBLE), CAST(get_json_object(s.consideration[1], '$.amount') AS DOUBLE)) AS royalty_fee_amount_raw
    , LEAST(CAST(get_json_object(s.consideration[0], '$.amount') AS DOUBLE), CAST(get_json_object(s.consideration[1], '$.amount') AS DOUBLE))/POWER(10, 18) AS royalty_fee_amount
    , pu.price*LEAST(CAST(get_json_object(s.consideration[0], '$.amount') AS DOUBLE), CAST(get_json_object(s.consideration[1], '$.amount') AS DOUBLE))/POWER(10, 18) AS royalty_fee_amount_usd
    , 100.0*LEAST(CAST(get_json_object(s.consideration[0], '$.amount') AS DOUBLE), CAST(get_json_object(s.consideration[1], '$.amount') AS DOUBLE))
        /CAST(CAST(get_json_object(s.consideration[0], '$.amount') AS DOUBLE)+CAST(get_json_object(s.consideration[1], '$.amount') AS DOUBLE) AS DECIMAL(38,0)) AS royalty_fee_percentage
    , CASE WHEN get_json_object(s.consideration[0], '$.token')='0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE currency_tok.symbol END AS royalty_fee_currency_symbol
    , CASE WHEN get_json_object(s.consideration[0], '$.recipient')!=s.recipient THEN get_json_object(s.consideration[0], '$.recipient')
        ELSE get_json_object(s.consideration[1], '$.recipient')
        END AS royalty_fee_receive_address
    , CAST('ethereum-blur-v1-' || s.evt_block_number || '-' || s.evt_tx_hash || '-' || s.evt_index AS string) AS unique_trade_id
FROM {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} s
INNER JOIN {{ source('ethereum', 'transactions') }} tx ON tx.block_number=s.evt_block_number
    AND tx.hash=s.evt_tx_hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND tx.block_time >= '{{seaport_usage_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tok ON nft_tok.contract_address=get_json_object(s.offer[0], '$.token')
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} currency_tok ON currency_tok.contract_address=get_json_object(s.consideration[0], '$.token')
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON ((pu.contract_address=get_json_object(s.consideration[0], '$.token') AND pu.blockchain='ethereum')
        OR (get_json_object(s.consideration[0], '$.token')='0x0000000000000000000000000000000000000000'  AND pu.blockchain IS NULL AND pu.contract_address IS NULL AND pu.symbol='ETH'))
    AND pu.minute=date_trunc('minute', s.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute >= '{{seaport_usage_start_date}}'
    {% endif %}
WHERE s.zone='0x0000000000d80cfcb8dfcd8b2c4fd9c813482938'
{% if is_incremental() %}
AND s.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
AND s.evt_block_time >= '{{seaport_usage_start_date}}'
{% endif %}
;
