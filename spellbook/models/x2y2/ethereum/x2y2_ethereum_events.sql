{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

WITH aggregator_routed_x2y2_txs AS (
    SELECT inv.evt_block_time AS block_time
    , inv.evt_block_number AS block_number
    , prof.to AS seller
    , ROUND(bytea2numeric_v2(substring(get_json_object(inv.item, '$.data'), 195,64)),0) AS token_id
    , get_json_object(inv.item, '$.price') AS amount_raw
    , prof.currency AS currency_contract
    , prof.contract_address AS project_contract_address
    , '0x' || substring(get_json_object(inv.item, '$.data'), 155, 40) AS nft_contract_address
    , tokens.name AS collection
    , agg.name AS aggregator_name
    , agg.contract_address AS aggregator_address
    , inv.evt_tx_hash AS tx_hash
    , prof.evt_index
    , COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0) AS platform_fee_amount_raw
    , COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0) AS platform_fee_percentage
    , COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage')/1e6, 0) AS royalty_fee_amount_raw
    , COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage')/1e6, 0) AS royalty_fee_percentage
    , get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.to') AS royalty_fee_receive_address
    FROM {{ source('x2y2_ethereum','X2Y2_r1_evt_EvProfit') }} prof
    INNER JOIN {{ source('x2y2_ethereum','X2Y2_r1_evt_EvInventory') }} inv ON inv.itemHash = prof.itemHash
    LEFT JOIN {{ ref('tokens_ethereum_nft') }} tokens ON ('0x' || substring(get_json_object(inv.item, '$.data'), 155, 40)) = tokens.contract_address
    LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=taker
    WHERE taker IN (SELECT contract_address FROM {{ ref('nft_ethereum_aggregators') }})
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND prof.evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
    )

, direct_x2y2_txs AS (
    SELECT inv.evt_block_time AS block_time
    , inv.evt_block_number AS block_number
    , CASE WHEN inv.currency = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN maker ELSE taker END AS buyer
    , prof.to AS seller
    , ROUND(bytea2numeric_v2(substring(get_json_object(inv.item, '$.data'), 195,64)),0) AS token_id
    , get_json_object(inv.item, '$.price') AS amount_raw
    , prof.currency AS currency_contract
    , prof.contract_address AS project_contract_address
    , '0x' || substring(get_json_object(inv.item, '$.data'), 155, 40) AS nft_contract_address
    , tokens.name AS collection
    , NULL AS aggregator_name
    , NULL AS aggregator_address
    , inv.evt_tx_hash AS tx_hash
    , prof.evt_index
    , COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0) AS platform_fee_amount_raw
    , COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')/1e6, 0) AS platform_fee_percentage
    , COALESCE(get_json_object(inv.item, '$.price')*get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage')/1e6, 0) AS royalty_fee_amount_raw
    , COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage')/1e6, 0) AS royalty_fee_percentage
    , get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.to') AS royalty_fee_receive_address
    FROM  {{ source('x2y2_ethereum','X2Y2_r1_evt_EvProfit') }} prof 
    INNER JOIN {{ source('x2y2_ethereum','X2Y2_r1_evt_EvInventory') }} inv ON inv.itemHash=prof.itemHash
    LEFT JOIN {{ ref('tokens_ethereum_nft') }} tokens ON ('0x' || substring(get_json_object(inv.item, '$.data'), 155, 40)) = tokens.contract_address
    WHERE taker NOT IN (SELECT contract_address FROM {{ ref('nft_ethereum_aggregators') }})
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND prof.evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
    )

, aggregator_routed_x2y2_txs_formatted AS (
    SELECT block_time
    , block_number
    , e721.to AS buyer
    , seller
    , token_id
    , amount_raw
    , currency_contract
    , project_contract_address
    , nft_contract_address
    , collection
    , 'Bundle Trade' AS trade_type
    , aggregator_name
    , aggregator_address
    , tx_hash
    , txs.evt_index
    , platform_fee_amount_raw
    , platform_fee_percentage
    , royalty_fee_amount_raw
    , royalty_fee_percentage
    , royalty_fee_receive_address
    FROM aggregator_routed_x2y2_txs txs
    LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} e721 ON txs.tx_hash = e721.evt_tx_hash
        AND txs.token_id = e721.tokenId
        AND e721.contract_address = txs.project_contract_address
        AND to NOT IN (SELECT contract_address FROM {{ ref('nft_ethereum_aggregators') }})
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND e721.evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
   )

, direct_x2y2_txs_formated AS (
    SELECT block_time
    , block_number
    , buyer
    , seller
    , token_id
    , amount_raw
    , currency_contract
    , project_contract_address
    , nft_contract_address
    , collection
    , 'Single Item Trade' AS trade_type
    , aggregator_name
    , aggregator_address
    , tx_hash
    , evt_index
    , platform_fee_amount_raw
    , platform_fee_percentage
    , royalty_fee_amount_raw
    , royalty_fee_percentage
    , royalty_fee_receive_address
    FROM direct_x2y2_txs
    )

, all_x2y2_txs AS (
    SELECT * FROM aggregator_routed_x2y2_txs_formatted
    UNION
    SELECT * FROM direct_x2y2_txs_formated
    )

SELECT 'ethereum' AS blockchain
, 'X2Y2' AS project
, 'v1' AS version
, TRY_CAST(date_trunc('DAY', txs.block_time) AS date) AS block_date
, txs.block_time
, txs.block_number
, txs.token_id
, txs.collection
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN pu.price*txs.amount_raw/POWER(10, 18)
    ELSE pu.price*txs.amount_raw/POWER(10, pu.decimals)
    END AS amount_usd
, CASE WHEN erct.evt_block_time IS NOT NULL THEN 'erc721'
    ELSE 'erc1155'
    END AS token_standard
, trade_type
, 1 AS number_of_items
, CASE WHEN et.`from`=seller THEN 'Offer Accepted'
    ELSE 'Buy'
    END AS trade_category
, 'Trade' AS evt_type
, txs.seller
, txs.buyer
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN txs.amount_raw/POWER(10, 18)
    ELSE txs.amount_raw/POWER(10, pu.decimals)
    END AS amount_original
, txs.amount_raw
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN 'ETH'
    ELSE pu.symbol
    END AS currency_symbol
, txs.currency_contract
, txs.project_contract_address
, txs.nft_contract_address
, aggregator_name
, aggregator_address
, txs.tx_hash
, et.`from` AS tx_from
, et.`to` AS tx_to
, platform_fee_amount_raw
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN platform_fee_amount_raw/POWER(10, 18)
    ELSE platform_fee_amount_raw/POWER(10, pu.decimals)
    END AS platform_fee_amount
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN pu.price*platform_fee_amount_raw/POWER(10, 18)
    ELSE pu.price*platform_fee_amount_raw/POWER(10, pu.decimals)
    END AS platform_fee_amount_usd
, platform_fee_percentage
, royalty_fee_amount_raw
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN royalty_fee_amount_raw/POWER(10, 18)
    ELSE royalty_fee_amount_raw/POWER(10, pu.decimals)
    END AS royalty_fee_amount
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN pu.price*royalty_fee_amount_raw/POWER(10, 18)
    ELSE pu.price*royalty_fee_amount_raw/POWER(10, pu.decimals)
    END AS royalty_fee_amount_usd
, royalty_fee_percentage
, royalty_fee_receive_address
, CASE WHEN currency_contract='0x0000000000000000000000000000000000000000' THEN 'ETH'
    ELSE pu.symbol
    END AS royalty_fee_currency_symbol
, 'x2y2-' || txs.tx_hash || '-' || txs.nft_contract_address || txs.token_id || '-' || txs.seller || '-' || txs.evt_index || 'Trade' AS unique_trade_id
FROM all_x2y2_txs txs
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain='ethereum'
    AND date_trunc('minute', pu.minute)=date_trunc('minute', txs.block_time)
    AND (pu.contract_address=txs.currency_contract
        OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND txs.currency_contract='0x0000000000000000000000000000000000000000'))
LEFT JOIN {{ source('ethereum','transactions') }} et ON et.hash=txs.tx_hash
    {% if is_incremental() %}
    AND TRY_CAST(date_trunc('DAY', et.block_time) AS date) = TRY_CAST(date_trunc('DAY', txs.block_time) AS date)
    {% endif %}
LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} erct ON txs.project_contract_address=erct.contract_address
    AND erct.evt_tx_hash=txs.tx_hash
    AND erct.tokenId=txs.token_id
    AND erct.from=txs.seller