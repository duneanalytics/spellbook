{{ config(
    schema = 'rarible_polygon',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
    )
}}

{% set nft_start_date = "TIMESTAMP '2022-02-23'" %}

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
        bytearray_substring(from_hex(json_extract_scalar(leftAsset,'$.data')), 13, 20) AS nft_contract_address,
        bytearray_to_uint256(bytearray_substring(from_hex(json_extract_scalar(leftAsset,'$.data')), 33)) AS token_id,
        newRightFill AS number_of_items,
        CASE WHEN json_extract_scalar(leftAsset,'$.assetClass') = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721 0x973bb640: erc1155
        CASE WHEN json_extract_scalar(rightAsset,'$.assetClass') = '0xaaaebeba' THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            ELSE bytearray_substring(from_hex(json_extract_scalar(rightAsset,'$.data')), 13, 20)
        END AS currency_contract,
        newLeftFill AS amount_raw
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE json_extract_scalar(rightAsset,'$.assetClass') in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '7' day)
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
        bytearray_substring(from_hex(json_extract_scalar(rightAsset,'$.data')), 13, 20) AS nft_contract_address,
        bytearray_to_uint256(bytearray_substring(from_hex(json_extract_scalar(rightAsset,'$.data')), 33)) AS token_id,
        newLeftFill AS number_of_items,
        CASE WHEN json_extract_scalar(rightAsset,'$.assetClass') = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721 0x973bb640: erc1155
        CASE WHEN json_extract_scalar(leftAsset,'$.assetClass') = '0xaaaebeba' THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            ELSE bytearray_substring(from_hex(json_extract_scalar(leftAsset,'$.data')), 13, 20)
        END AS currency_contract,
        newRightFill AS amount_raw
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE json_extract_scalar(leftAsset,'$.assetClass') in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    UNION ALL

    -- directPurchase
    SELECT 'buy' AS trade_category,
        p.call_block_time AS evt_block_time,
        p.call_block_number AS evt_block_number,
        p.call_tx_hash AS evt_tx_hash,
        p.contract_address,
        integer '1' AS evt_index,
        'Trade' AS evt_type,
        t."from" AS buyer, --p.direct:nftData
        from_hex(json_extract_scalar(p.direct,'$.sellOrderMaker')) AS seller,
        bytearray_substring(from_hex(json_extract_scalar(p.direct,'$.nftData')), 13, 20) AS nft_contract_address,
        bytearray_to_uint256(bytearray_substring(from_hex(json_extract_scalar(p.direct,'$.nftData')), 53, 20)) AS token_id,
        CAST(json_extract_scalar(p.direct,'$.buyOrderNftAmount') as uint256) AS number_of_items,
        CASE WHEN json_extract_scalar(p.direct,'$.nftAssetClass') = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN from_hex(json_extract_scalar(p.direct,'$.paymentToken')) = 0x0000000000000000000000000000000000000000
            THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            ELSE from_hex(json_extract_scalar(p.direct,'$.paymentToken'))
        END AS currency_contract,
        cast(json_extract_scalar(p.direct,'$.buyOrderPaymentAmount') as uint256) AS amount_raw
    FROM {{ source ('rarible_polygon', 'ExchangeMetaV2_call_directPurchase') }} p
    INNER JOIN {{ source('polygon','transactions') }} t ON t.block_number = p.call_block_number
        AND t.hash = p.call_tx_hash
        {% if not is_incremental() %}
        AND t.block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE call_success = true
        {% if not is_incremental() %}
        AND p.call_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND p.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    UNION ALL

    -- directAcceptBid
    SELECT 'sell' AS trade_category,
        p.call_block_time AS evt_block_time,
        p.call_block_number AS evt_block_number,
        p.call_tx_hash AS evt_tx_hash,
        p.contract_address,
        integer '1' AS evt_index,
        'Trade' AS evt_type,
        from_hex(json_extract_scalar(p.direct,'$.bidMaker')) AS buyer,
        t."from" AS seller,
        bytearray_substring(from_hex(json_extract_scalar(p.direct,'$.nftData')), 13, 20) AS nft_contract_address,
        bytearray_to_uint256(bytearray_substring(from_hex(json_extract_scalar(p.direct,'$.nftData')), 53, 20)) AS token_id,
        CAST(json_extract_scalar(p.direct,'$.buyOrderNftAmount') as uint256) AS number_of_items,
        CASE WHEN json_extract_scalar(p.direct,'$.nftAssetClass') = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN from_hex(json_extract_scalar(p.direct,'$.paymentToken')) = 0x0000000000000000000000000000000000000000
            THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            ELSE from_hex(json_extract_scalar(p.direct,'$.paymentToken'))
        END AS currency_contract,
        cast(json_extract_scalar(p.direct,'$.sellOrderPaymentAmount') as uint256) AS amount_raw
    FROM {{ source('rarible_polygon','ExchangeMetaV2_call_directAcceptBid') }} p
    INNER JOIN {{ source('polygon','transactions') }} t ON t.block_number = p.call_block_number
        AND t.hash = p.call_tx_hash
        {% if not is_incremental() %}
        AND t.block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE call_success = true
        {% if not is_incremental() %}
        AND p.call_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND p.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

-- note: this logic will probably not hold for multi trade transactions
trade_amount_detail as (
    SELECT e.block_number AS evt_block_number,
        e.tx_hash AS evt_tx_hash,
        e.value as amount_raw,
        row_number() OVER (PARTITION BY e.tx_hash ORDER BY e.trace_address) AS item_index
    FROM {{ source('polygon', 'traces') }} e
    INNER JOIN trades t ON e.block_number = t.evt_block_number
        AND e.tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND e.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE t.currency_contract = 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
        AND e.value > uint256 '0'
        AND cardinality(trace_address) > 0 -- exclude the main call record

    UNION ALL

    SELECT e.evt_block_number,
        e.evt_tx_hash,
        e.value AS amount_raw,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS item_index
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN trades t ON e.evt_block_number = t.evt_block_number
        AND e.evt_tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE t.currency_contract != 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
),

trade_amount_summary as (
    SELECT evt_block_number,
        evt_tx_hash,
        amount_raw,
        -- When there is royalty fee, it is the first transfer
        (case when transfer_count >= 4 then amount_raw_2 else amount_raw_1 end) AS platform_fee_amount_raw,
        (case when transfer_count >= 4 then amount_raw_1 else uint256 '0' end) AS royalty_fee_amount_raw
    FROM (
        SELECT evt_block_number,
            evt_tx_hash,
            sum(amount_raw) AS amount_raw,
            sum(case when item_index = 1 then amount_raw else uint256 '0' end) AS amount_raw_1,
            sum(case when item_index = 2 then amount_raw else uint256 '0' end) AS amount_raw_2,
            sum(case when item_index = 3 then amount_raw else uint256 '0' end) AS amount_raw_3,
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
  a.evt_block_time AS block_time,
  a.evt_block_number AS block_number,
  coalesce(s.amount_raw,uint256 '0') / power(10, erc.decimals) * p.price AS amount_usd,
  coalesce(s.amount_raw,uint256 '0') / power(10, erc.decimals) AS amount_original,
  coalesce(s.amount_raw,uint256 '0') as amount_raw,
  CASE WHEN erc.symbol = 'WMATIC' THEN 'MATIC' ELSE erc.symbol END AS currency_symbol,
  a.currency_contract,
  token_id,
  token_standard,
  a.contract_address AS project_contract_address,
  evt_type,
  CAST(NULL AS varchar) AS collection,
  CASE WHEN number_of_items = uint256 '1' THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
  number_of_items,
  a.trade_category,
  a.buyer,
  a.seller,
  a.nft_contract_address,
  agg.name AS aggregator_name,
  agg.contract_address AS aggregator_address,
  t."from" AS tx_from,
  t."to" AS tx_to,
  coalesce(s.platform_fee_amount_raw,uint256 '0') as platform_fee_amount_raw,
  CAST(coalesce(s.platform_fee_amount_raw,uint256 '0') / power(10, erc.decimals) AS double) AS platform_fee_amount,
  CAST(coalesce(s.platform_fee_amount_raw,uint256 '0') / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
  CAST(coalesce(s.platform_fee_amount_raw,uint256 '0')  / s.amount_raw * 100 as double) as platform_fee_percentage,
  coalesce(s.royalty_fee_amount_raw,uint256 '0') AS royalty_fee_amount_raw,
  CAST(coalesce(s.royalty_fee_amount_raw,uint256 '0') / power(10, erc.decimals) as double) AS royalty_fee_amount,
  CAST(coalesce(s.royalty_fee_amount_raw,uint256 '0') / power(10, erc.decimals) * p.price AS double) AS royalty_fee_amount_usd,
  CAST(coalesce(s.royalty_fee_amount_raw,uint256 '0') / s.amount_raw * 100 AS double) AS royalty_fee_percentage,
  cast(null as varbinary) AS royalty_fee_receive_address,
  cast(null as varchar) AS royalty_fee_currency_symbol,
  cast(a.evt_tx_hash as varchar) || '-' || cast(a.evt_index as varchar) AS unique_trade_id
FROM trades a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= {{nft_start_date}}
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN trade_amount_summary s ON a.evt_block_number = s.evt_block_number AND a.evt_tx_hash = s.evt_tx_hash
LEFT JOIN {{ ref('tokens_erc20') }} erc ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN {{ source('prices', 'usd') }} p ON p.contract_address = a.currency_contract AND p.minute = date_trunc('minute', a.evt_block_time)
    {% if not is_incremental() %}
    AND p.minute >= {{nft_start_date}}
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t."to"
