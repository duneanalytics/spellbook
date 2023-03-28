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

{%- set project_start_date = '2022-02-04' %}
{%- set eth_erc20_addr = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{%- set fee_management_addr = '0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd' %}

-- base sources
WITH
src_evt_profit as (
    SELECT
     *
     , CASE WHEN currency='0x0000000000000000000000000000000000000000'
        THEN true ELSE false END as is_native_eth
    FROM {{ source('x2y2_ethereum','X2Y2_r1_evt_EvProfit') }}
    WHERE evt_block_time >= '{{project_start_date}}'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

src_evt_inventory as (
    SELECT
     evt_tx_hash
    ,evt_block_time
    ,itemHash
    ,taker
    ,maker
    ,get_json_object(inv.item, '$.data') as data
    ,bytea2numeric_v3(substring(get_json_object(inv.item, '$.data'), 195,64)) as token_id
    ,'0x' || substring(get_json_object(inv.item, '$.data'), 155, 40) as nft_contract_address
    ,get_json_object(inv.detail, '$.executionDelegate') as execution_delegate
    ,get_json_object(inv.item, '$.price') as price
    ,get_json_object(inv.detail, '$.fees[0]') as fees_0
    ,get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.to') as fees_0_to
    ,get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage') as platform_fee_percentage
    ,get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.to') as fees_1_to
    , COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[4]'), '$.percentage'), 0)
      as royalty_fee_percentage
    , COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)
      as all_fee_percentage
    FROM {{ source('x2y2_ethereum','X2Y2_r1_evt_EvInventory') }} inv
    WHERE evt_block_time >= '{{project_start_date}}'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

src_eth_transactions as  (
    SELECT *
    FROM {{ source('ethereum','transactions') }}
    WHERE block_time > '{{project_start_date}}'
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

src_prices_usd as (
    SELECT *
    FROM {{ source('prices','usd') }}
    WHERE blockchain = 'ethereum'
        AND minute > '{{project_start_date}}'
        {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}

)






-- results
SELECT 'ethereum' AS blockchain
, 'x2y2' AS project
, 'v1' AS version
, prof.evt_block_time AS block_time
, date_trunc('day', prof.evt_block_time) AS block_date
, prof.evt_block_number AS block_number
, inv.token_id as token_id
, nft_token.name AS collection
, CAST(inv.price AS DECIMAL(38,0)) AS amount_raw
, inv.price/POWER(10, currency_token.decimals) AS amount_original
, pu.price*(inv.price/POWER(10, currency_token.decimals)) AS amount_usd
, nft_token.standard as token_standard
, 'Single Item Trade' AS trade_type
, CAST(1 AS DECIMAL(38,0)) AS number_of_items
, CASE WHEN (inv.fees_0 IS NULL OR inv.fees_0_to != '{{fee_management_addr}}') AND (prof.evt_block_time < '2022-04-01' OR prof.evt_block_time >= '2022-05-01') THEN 'Private Sale'
    WHEN (et.from=inv.maker or inv.maker = agg.contract_address) THEN 'Offer Accepted'
    ELSE 'Buy'
    END AS trade_category
, 'Trade' AS evt_type
, CASE WHEN inv.taker = agg.contract_address THEN et.from ELSE inv.taker END AS buyer
, CASE WHEN inv.maker = agg.contract_address THEN et.from ELSE inv.maker END AS seller
, CASE WHEN prof.is_native_eth THEN 'ETH'
    ELSE currency_token.symbol
    END AS currency_symbol
, CASE WHEN prof.is_native_eth THEN '{{eth_erc20_addr}}'
    ELSE prof.currency
    END AS currency_contract
, inv.nft_contract_address
, prof.contract_address AS project_contract_address
, COALESCE(agg_m.aggregator_name, agg.name) AS aggregator_name
, agg.contract_address AS aggregator_address
, prof.evt_tx_hash AS tx_hash
, et.from AS tx_from
, et.to AS tx_to
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN ROUND(COALESCE(inv.price*inv.platform_fee_percentage/1e6, 0), 0)
    ELSE 0
    END AS platform_fee_amount_raw
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN ROUND(COALESCE(inv.price*inv.platform_fee_percentage/1e6, 0), 0)/POWER(10, currency_token.decimals)
    ELSE 0
    END AS platform_fee_amount
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN pu.price*ROUND(COALESCE(inv.price*inv.platform_fee_percentage/1e6), 0)/POWER(10, currency_token.decimals)
    ELSE 0
    END AS platform_fee_amount_usd
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN COALESCE(inv.platform_fee_percentage/1e4, 0)
    ELSE 0
    END AS platform_fee_percentage
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN COALESCE(inv.price*inv.royalty_fee_percentage/1e6, 0)
    ELSE COALESCE(inv.price*inv.all_fee_percentage/1e6, 0)
    END AS royalty_fee_amount_raw
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN COALESCE(inv.price*inv.royalty_fee_percentage/1e6, 0)/POWER(10, currency_token.decimals)
    ELSE COALESCE(inv.price*inv.all_fee_percentage/1e6, 0)/POWER(10, currency_token.decimals)
    END AS royalty_fee_amount
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN pu.price*COALESCE(inv.price*inv.royalty_fee_percentage/1e6, 0)/POWER(10, currency_token.decimals)
    ELSE pu.price*COALESCE(inv.price*inv.all_fee_percentage/1e6, 0)/POWER(10, currency_token.decimals)
    END AS royalty_fee_amount_usd
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN inv.royalty_fee_percentage/1e4
    ELSE inv.all_fee_percentage/1e4
    END AS royalty_fee_percentage
, CASE WHEN prof.is_native_eth THEN 'ETH'
    ELSE currency_token.symbol
    END AS royalty_fee_currency_symbol
, CASE WHEN inv.fees_0_to='{{fee_management_addr}}' THEN inv.fees_1_to
    ELSE inv.fees_0_to
    END AS royalty_fee_receive_address
, 'ethereum-x2y2-v1' || '-' || prof.evt_block_number || '-' || prof.evt_tx_hash || '-' ||  prof.evt_index AS unique_trade_id
FROM src_evt_profit prof
INNER JOIN src_evt_inventory inv
    ON inv.evt_block_time=prof.evt_block_time
      AND inv.itemHash = prof.itemHash
INNER JOIN src_eth_transactions et
    ON et.block_time=prof.evt_block_time
      AND et.hash=prof.evt_tx_hash
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_token ON inv.nft_contract_address = nft_token.contract_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} currency_token ON currency_token.contract_address=prof.currency
        OR (prof.is_native_eth AND currency_token.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN src_prices_usd pu
    ON pu.minute=date_trunc('minute', prof.evt_block_time)
    AND (pu.contract_address=prof.currency
        OR (prof.is_native_eth AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
        ON RIGHT(et.data, agg_m.hash_marker_size) = agg_m.hash_marker

