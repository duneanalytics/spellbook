{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nftx",
                                \'["hildobby"]\') }}')
}}

{% set project_start_date = '2021-06-21' %}

WITH mints_and_redemptions AS (
    SELECT 'Sell' AS trade_category
    , contract_address
    , tx_hash
    , evt_index
    , block_time
    , block_number
    , to
    , ids_and_count.nftIds AS token_id
    , CASE WHEN ids_and_count.amounts < 1 OR ids_and_count.amounts IS NULL THEN 1
        ELSE ids_and_count.amounts END AS amount
    FROM (
        SELECT contract_address
        , evt_tx_hash AS tx_hash
        , evt_index
        , evt_block_time AS block_time
        , evt_block_number AS block_number
        , to
        , explode(arrays_zip(nftIds, amounts)) AS ids_and_count
        FROM {{ source('nftx_v2_ethereum','NFTXVaultUpgradeable_v1_evt_Minted') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        WHERE evt_block_time > '{{project_start_date}}'
        {% endif %}
        )
    
    UNION ALL
    
    SELECT 'Buy' AS trade_category
    , contract_address
    , evt_tx_hash AS tx_hash
    , evt_index
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , to
    , CAST(specificIds AS string) AS token_id
    , 1 AS amount
    FROM {{ source('nftx_v2_ethereum','NFTXVaultUpgradeable_v1_evt_Redeemed') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time > '{{project_start_date}}'
    {% endif %}
    )

, pool_trades AS (
    SELECT 'Buy' AS trade_category
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_index
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , to AS pool
    , ethSpent AS amount_raw
    , count AS number_of_items
    FROM {{ source('nftx_v2_ethereum','NFTXMarketplaceZap_evt_Buy') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time > '{{project_start_date}}'
    {% endif %}
    
    UNION ALL
    
    SELECT 'Sell' AS trade_category
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_index
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , to AS pool
    , ethReceived AS amount_raw
    , count AS number_of_items
    FROM {{ source('nftx_v2_ethereum','NFTXMarketplaceZap_evt_Sell') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time > '{{project_start_date}}'
    {% endif %}
    
    UNION ALL
    
    SELECT 'Swap' AS trade_category
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_index
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , to AS pool
    , ethSpent AS amount_raw
    , count AS number_of_items
    FROM {{ source('nftx_v2_ethereum','NFTXMarketplaceZap_evt_Swap') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time > '{{project_start_date}}'
    {% endif %}
    )

SELECT distinct 'ethereum' AS blockchain
, 'nftx' AS project
, 'v1' AS version
, mar.block_time
, date_trunc('day', mar.block_time) AS block_date
, mar.block_number
, CASE WHEN mar.amount=1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type
, mar.trade_category
, 'Trade' AS evt_type
, trans.from AS seller
, trans.to AS buyer
, trans.contract_address AS nft_contract_address
, tok.name AS collection
, mar.token_id
, trans.token_standard
, mar.amount AS number_of_items
, CASE WHEN sushi.token_bought_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN sushi.token_sold_address
    ELSE sushi.token_bought_address
    END AS currency_contract
, CASE WHEN sushi.token_bought_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN sushi.token_sold_symbol
    ELSE sushi.token_bought_symbol
    END AS currency_symbol
, CASE WHEN sushi.token_bought_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN sushi.token_sold_amount_raw
    ELSE sushi.token_bought_amount_raw
    END AS amount_raw
, CASE WHEN sushi.token_bought_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN sushi.token_sold_amount
    ELSE sushi.token_bought_amount
    END AS amount_original
, CASE WHEN sushi.token_bought_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN sushi.token_bought_amount*pu.price
    ELSE sushi.token_sold_amount*pu.price
    END AS amount_usd
, mar.contract_address AS project_contract_address
, COALESCE(agg.name, agg_m.aggregator_name) AS aggregator_name
, agg.contract_address AS aggregator_address
, mar.tx_hash
, et.from AS tx_from
, et.to AS tx_to
, 0 AS platform_fee_amount_raw
, 0 AS platform_fee_amount
, 0 AS platform_fee_amount_usd
, 0 AS platform_fee_percentage
, 0 AS royalty_fee_currency_symbol
, 0 AS royalty_fee_amount_raw
, 0 AS royalty_fee_amount
, 0 AS royalty_fee_amount_usd
, 0 AS royalty_fee_percentage
, 0 AS royalty_fee_receive_address
, 'ethereum-nftx-' || mar.block_number || '-' || mar.tx_hash || '-' || mar.evt_index AS unique_trade_id
FROM mints_and_redemptions mar
INNER JOIN {{ source('ethereum', 'transactions') }} et ON et.block_number=mar.block_number
        AND et.hash=mar.tx_hash
        {% if is_incremental() %}
        AND et.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND et.block_time > '{{project_start_date}}'
        {% endif %}
LEFT JOIN {{ ref('sushiswap_ethereum_trades') }} sushi ON sushi.block_time=mar.block_time
    AND sushi.tx_hash=mar.tx_hash
    AND mar.contract_address IN (sushi.token_sold_address, sushi.token_bought_address)
    {% if is_incremental() %}
    AND sushi.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND sushi.block_time > '{{project_start_date}}'
    {% endif %}
RIGHT JOIN {{ ref('nft_ethereum_transfers') }} trans ON trans.block_number=mar.block_number
    AND trans.tx_hash=mar.tx_hash
    {% if is_incremental() %}
    AND trans.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND trans.block_time > '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
    ON RIGHT(et.data, agg_m.hash_marker_size) = agg_m.hash_marker
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND pu.minute=date_trunc('minute', mar.block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute > '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tok ON tok.contract_address=trans.contract_address

UNION ALL

SELECT distinct 'ethereum' AS blockchain
, 'nftx' AS project
, 'v1' AS version
, t.block_time
, date_trunc('day', t.block_time) AS block_date
, t.block_number
, CASE WHEN trans.amount=1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type
, t.trade_category
, 'Trade' AS evt_type
, trans.from AS seller
, trans.to AS buyer
, trans.contract_address AS nft_contract_address
, tok.name AS collection
, trans.token_id AS token_id
, trans.token_standard
, trans.amount AS number_of_items
, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
, 'ETH' AS currency_symbol
, t.amount_raw*(trans.amount/t.number_of_items) AS amount_raw
, t.amount_raw/POWER(10, 18)*(trans.amount/t.number_of_items) AS amount_original
, pu.price*t.amount_raw/POWER(10, 18)*(trans.amount/t.number_of_items) amount_usd
, t.project_contract_address
, COALESCE(agg.name, agg_m.aggregator_name) AS aggregator_name
, agg.contract_address AS aggregator_address
, t.tx_hash
, et.from AS tx_from
, et.to AS tx_to
, 0 AS platform_fee_amount_raw
, 0 AS platform_fee_amount
, 0 AS platform_fee_amount_usd
, 0 AS platform_fee_percentage
, 0 AS royalty_fee_currency_symbol
, 0 AS royalty_fee_amount_raw
, 0 AS royalty_fee_amount
, 0 AS royalty_fee_amount_usd
, 0 AS royalty_fee_percentage
, 0 AS royalty_fee_receive_address
, 'ethereum-nftx-' || t.block_number || '-' || t.tx_hash || '-' || trans.evt_index AS unique_trade_id
FROM pool_trades t
INNER JOIN {{ source('ethereum','transactions') }} et ON et.block_number=t.block_number
    AND et.hash=t.tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND et.block_time > '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND pu.minute=date_trunc('minute', t.block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute > '{{project_start_date}}'
    {% endif %}
RIGHT JOIN {{ ref('nft_ethereum_transfers') }} trans ON trans.block_number=t.block_number
    AND trans.tx_hash=t.tx_hash
    AND t.pool IN (trans.from, trans.to)
    {% if is_incremental() %}
    AND trans.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND trans.block_time > '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tok ON tok.contract_address=trans.contract_address
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
    ON RIGHT(et.data, agg_m.hash_marker_size) = agg_m.hash_marker