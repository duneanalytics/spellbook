{{ config(
    schema = 'rarible_polygon',
    alias = alias('events'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
    )
}}

{% set nft_start_date = "2022-02-23" %}

WITH trades AS (
    select 'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        rightMaker AS buyer,
        leftMaker AS seller,
        '0x' || right(substring(leftAsset:data, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(leftAsset:data, 3 + 64, 64)) AS string) AS token_id,
        newRightFill AS number_of_items,
        CASE WHEN leftAsset:assetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN rightAsset:assetClass = '0xaaaebeba' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE '0x' || right(substring(rightAsset:data, 3, 64), 40)
        END AS currency_contract,
        newLeftFill AS amount_raw
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE rightAsset:assetClass in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC; 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    select 'sell' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        leftMaker AS buyer,
        rightMaker AS seller,
        '0x' || right(substring(rightAsset:data, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(rightAsset:data, 3 + 64, 64)) AS string) AS token_id,
        newLeftFill AS number_of_items,
        CASE WHEN rightAsset:assetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN leftAsset:assetClass = '0xaaaebeba' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE '0x' || right(substring(leftAsset:data, 3, 64), 40)
        END AS currency_contract,
        newRightFill AS amount_raw
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE leftAsset:assetClass in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC; 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- directPurchase
    SELECT 'buy' AS trade_category,
        p.call_block_time AS evt_block_time,
        p.call_block_number AS evt_block_number,
        p.call_tx_hash AS evt_tx_hash,
        p.contract_address,
        CAST(-1 as integer) AS evt_index,
        'Trade' AS evt_type,
        t.`from` AS buyer,
        p.direct:sellOrderMaker AS seller,
        '0x' || right(substring(p.direct:nftData, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(p.direct:nftData, 3 + 64, 64)) AS string) AS token_id,
        p.direct:buyOrderNftAmount AS number_of_items,
        CASE WHEN p.direct:nftAssetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN p.direct:paymentToken = '0x0000000000000000000000000000000000000000'
            THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE p.direct:paymentToken
        END AS currency_contract,
        p.direct:buyOrderPaymentAmount AS amount_raw
    FROM {{ source ('rarible_polygon', 'ExchangeMetaV2_call_directPurchase') }} p
    INNER JOIN {{ source('polygon','transactions') }} t ON t.block_number = p.call_block_number
        AND t.hash = p.call_tx_hash
        {% if not is_incremental() %}
        AND t.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE call_success = true
        {% if not is_incremental() %}
        AND p.call_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND p.call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- directAcceptBid
    SELECT 'sell' AS trade_category,
        p.call_block_time AS evt_block_time,
        p.call_block_number AS evt_block_number,
        p.call_tx_hash AS evt_tx_hash,
        p.contract_address,
        CAST(-1 as integer) AS evt_index,
        'Trade' AS evt_type,
        p.direct:bidMaker AS buyer,
        t.`from` AS seller,
        '0x' || right(substring(p.direct:nftData, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(p.direct:nftData, 3 + 64, 64)) AS string) AS token_id,
        p.direct:sellOrderNftAmount AS number_of_items,
        CASE WHEN p.direct:nftAssetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN p.direct:paymentToken = '0x0000000000000000000000000000000000000000'
            THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE p.direct:paymentToken
        END AS currency_contract,
        p.direct:sellOrderPaymentAmount AS amount_raw
    FROM {{ source('rarible_polygon','ExchangeMetaV2_call_directAcceptBid') }} p
    INNER JOIN {{ source('polygon','transactions') }} t ON t.block_number = p.call_block_number
        AND t.hash = p.call_tx_hash
        {% if not is_incremental() %}
        AND t.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE call_success = true
        {% if not is_incremental() %}
        AND p.call_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND p.call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

-- note: this logic will probably not hold for multi trade transactions
trade_amount_detail as (
    SELECT e.block_number AS evt_block_number,
        e.tx_hash AS evt_tx_hash,
        cast(e.value AS double) as amount_raw,
        row_number() OVER (PARTITION BY e.tx_hash ORDER BY e.trace_address) AS item_index
    FROM {{ source('polygon', 'traces') }} e
    INNER JOIN trades t ON e.block_number = t.evt_block_number
        AND e.tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE t.currency_contract = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
        AND cast(e.value as double) > 0
        AND cardinality(trace_address) > 0 -- exclude the main call record

    UNION ALL

    SELECT e.evt_block_number,
        e.evt_tx_hash,
        CAST(e.value as double) AS amount_raw,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS item_index
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN trades t ON e.evt_block_number = t.evt_block_number
        AND e.evt_tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE t.currency_contract <> '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
),

trade_amount_summary as (
    SELECT evt_block_number,
        evt_tx_hash,
        amount_raw,
        -- When there is royalty fee, it is the first transfer
        (case when transfer_count >= 4 then amount_raw_2 else amount_raw_1 end) AS platform_fee_amount_raw,
        (case when transfer_count >= 4 then amount_raw_1 else 0 end) AS royalty_fee_amount_raw
    FROM (
        SELECT evt_block_number,
            evt_tx_hash,
            sum(amount_raw) AS amount_raw,
            sum(case when item_index = 1 then amount_raw else 0 end) AS amount_raw_1,
            sum(case when item_index = 2 then amount_raw else 0 end) AS amount_raw_2,
            sum(case when item_index = 3 then amount_raw else 0 end) AS amount_raw_3,
            count(*) as transfer_count
        FROM trade_amount_detail
        GROUP BY 1, 2
    )
)

SELECT
  'polygon' AS blockchain,
  'rarible' AS project,
  'v2' AS version,
  a.evt_tx_hash AS tx_hash,
  date_trunc('day', a.evt_block_time) AS block_date,
  a.evt_block_time AS block_time,
  a.evt_block_number AS block_number,
  coalesce(s.amount_raw,0) / power(10, erc.decimals) * p.price AS amount_usd,
  coalesce(s.amount_raw,0) / power(10, erc.decimals) AS amount_original,
  coalesce(s.amount_raw,0) as amount_raw,
  CASE WHEN erc.symbol = 'WMATIC' THEN 'MATIC' ELSE erc.symbol END AS currency_symbol,
  a.currency_contract,
  token_id,
  token_standard,
  a.contract_address AS project_contract_address,
  evt_type,
  CAST(NULL AS string) AS collection,
  CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
  CAST(number_of_items AS decimal(38,0)) AS number_of_items,
  a.trade_category,
  a.buyer,
  a.seller,
  a.nft_contract_address,
  agg.name AS aggregator_name,
  agg.contract_address AS aggregator_address,
  t.`from` AS tx_from,
  t.`to` AS tx_to,
  coalesce(s.platform_fee_amount_raw,0) as platform_fee_amount_raw,
  CAST(coalesce(s.platform_fee_amount_raw,0) / power(10, erc.decimals) AS double) AS platform_fee_amount,
  CAST(coalesce(s.platform_fee_amount_raw,0) / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
  CAST(coalesce(s.platform_fee_amount_raw,0)  / s.amount_raw * 100 as double) as platform_fee_percentage,
  CAST(coalesce(s.royalty_fee_amount_raw,0) as double) AS royalty_fee_amount_raw,
  CAST(coalesce(s.royalty_fee_amount_raw,0) / power(10, erc.decimals) as double) AS royalty_fee_amount,
  CAST(coalesce(s.royalty_fee_amount_raw,0) / power(10, erc.decimals) * p.price AS double) AS royalty_fee_amount_usd,
  CAST(coalesce(s.royalty_fee_amount_raw,0) / s.amount_raw * 100 AS double) AS royalty_fee_percentage,
  CAST(NULL AS varchar(5)) AS royalty_fee_receive_address,
  CAST(NULL AS string)  AS royalty_fee_currency_symbol,
  a.evt_tx_hash || '-' || a.evt_type || '-' || a.evt_index || '-' || a.token_id || '-' || CAST(a.number_of_items AS string)  AS unique_trade_id
FROM trades a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN trade_amount_summary s ON a.evt_block_number = s.evt_block_number AND a.evt_tx_hash = s.evt_tx_hash
LEFT JOIN {{ ref('tokens_erc20') }} erc ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN {{ source('prices', 'usd') }} p ON p.contract_address = a.currency_contract AND p.minute = date_trunc('minute', a.evt_block_time)
    {% if not is_incremental() %}
    AND p.minute >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
