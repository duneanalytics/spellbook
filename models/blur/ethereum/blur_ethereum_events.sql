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

SELECT
    'ethereum' AS blockchain
    , 'blur' AS project
    , 'v1' AS version
    , date_trunc('day', bm.evt_block_time) AS block_date
    , bm.evt_block_time AS block_time
    , bm.evt_block_number AS block_number
    , get_json_object(bm.sell, '$.tokenId') AS token_id
    , erct.token_standard
    , nft.name AS collection
    , CASE WHEN get_json_object(bm.buy, '$.amount')=1 THEN 'Single Item Trade'
        ELSE 'Bundle Trade'
        END AS trade_type
    , CAST(get_json_object(bm.buy, '$.amount') AS DECIMAL(38,0)) AS number_of_items
    , 'Trade' AS evt_type
    , COALESCE(seller_fix.from, get_json_object(bm.sell, '$.trader')) AS seller
    , COALESCE(buyer_fix.to, get_json_object(bm.buy, '$.trader')) AS buyer
    , CASE WHEN get_json_object(bm.buy, '$.matchingPolicy') IN ('0x00000000006411739da1c40b106f8511de5d1fac', '0x0000000000dab4a563819e8fd93dba3b25bc3495') THEN 'Buy'
        WHEN get_json_object(bm.buy, '$.matchingPolicy') IN ('0x0000000000b92d5d043faf7cecf7e2ee6aaed232') THEN 'Offer Accepted'
        WHEN et.from=buyer_fix.to OR et.from=COALESCE(buyer_fix.to, get_json_object(bm.buy, '$.trader')) THEN 'Buy'
        ELSE 'Offer Accepted'
        END AS trade_category
    , CAST(get_json_object(bm.buy, '$.price') AS DECIMAL(38,0)) AS amount_raw
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN get_json_object(bm.buy, '$.price')/POWER(10, 18)
        ELSE get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals)
        END AS amount_original
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN pu.price*get_json_object(bm.buy, '$.price')/POWER(10, 18)
        ELSE pu.price*get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals)
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
    , COALESCE(get_json_object(bm.buy, '$.price')*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS royalty_fee_amount_raw
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN COALESCE(get_json_object(bm.buy, '$.price')/POWER(10, 18)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0)
        ELSE COALESCE(get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0)
        END AS royalty_fee_amount
    , CASE WHEN get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN COALESCE(pu.price*get_json_object(bm.buy, '$.price')/POWER(10, 18)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0)
        ELSE COALESCE(pu.price*get_json_object(bm.buy, '$.price')/POWER(10, pu.decimals)*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0)
        END AS royalty_fee_amount_usd
    , CAST(COALESCE(get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/100, 0) AS DOUBLE) AS royalty_fee_percentage
    , get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.recipient') AS royalty_fee_receive_address
    , CASE WHEN get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.recipient') IS NOT NULL AND get_json_object(bm.buy, '$.paymentToken') IN ('0x0000000000000000000000000000000000000000', '0x0000000000a39bb272e79075ade125fd351887ac') THEN 'ETH'
        WHEN get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.recipient') IS NOT NULL THEN pu.symbol
        END AS royalty_fee_currency_symbol
    ,  'ethereum-blur-v1-' || bm.evt_block_number || '-' || bm.evt_tx_hash || '-' || bm.evt_index AS unique_trade_id
FROM {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }} bm
JOIN {{ source('ethereum','transactions') }} et ON et.block_time=bm.evt_block_time
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
LEFT JOIN {{ ref('nft_ethereum_transfers') }} erct ON erct.block_time=bm.evt_block_time
    AND get_json_object(bm.buy, '$.collection')=erct.contract_address
    AND erct.tx_hash=bm.evt_tx_hash
    AND get_json_object(bm.sell, '$.tokenId')=erct.token_id
    AND erct.from=get_json_object(bm.sell, '$.trader')
    {% if not is_incremental() %}
    AND erct.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND erct.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} buyer_fix ON buyer_fix.block_time=bm.evt_block_time
    AND get_json_object(bm.buy, '$.collection')=buyer_fix.contract_address
    AND buyer_fix.tx_hash=bm.evt_tx_hash
    AND get_json_object(bm.sell, '$.tokenId')=buyer_fix.token_id
    AND get_json_object(bm.buy, '$.trader')=agg.contract_address
    AND buyer_fix.from=agg.contract_address
    {% if not is_incremental() %}
    AND buyer_fix.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND buyer_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_transfers') }} seller_fix ON seller_fix.block_time=bm.evt_block_time
    AND get_json_object(bm.buy, '$.collection')=seller_fix.contract_address
    AND seller_fix.tx_hash=bm.evt_tx_hash
    AND get_json_object(bm.sell, '$.tokenId')=seller_fix.token_id
    AND get_json_object(bm.sell, '$.trader')=agg.contract_address
    AND seller_fix.to=agg.contract_address
    {% if not is_incremental() %}
    AND seller_fix.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND seller_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE bm.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
;
