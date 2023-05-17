{{ config(
    schema = 'looksrare_v2_ethereum',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

{% set looksrare_v2_start_date = '2023-04-01' %}

WITH looksrare_v2_trades AS (
    SELECT l.evt_block_time AS block_time
    , l.evt_block_number AS block_number
    , 'Offer Accepted' AS trade_category
    , l.feeAmounts[0]+l.feeAmounts[1]+l.feeAmounts[2] AS amount_raw
    , l.askUser AS seller
    , l.bidUser AS buyer
    , l.collection AS nft_contract_address
    , element_at(l.amounts, 1) AS number_of_items
    , l.currency
    , l.itemIds[0] AS token_id
    , l.contract_address AS project_contract_address
    , l.evt_tx_hash AS tx_hash
    , l.evt_index
    , l.feeAmounts[1] AS royalty_fee_amount_raw
    , l.feeAmounts[2] AS platform_fee_amount_raw
    , CASE WHEN l.feeRecipients[1]!='0x0000000000000000000000000000000000000000' THEN l.feeRecipients[1] END AS royalty_fee_receive_address
    , get_json_object(l.nonceInvalidationParameters, '$.orderHash') AS order_hash
    FROM {{ source('looksrare_v2_ethereum','LooksRareProtocol_evt_TakerAsk') }} l
    {% if is_incremental() %}
    WHERE l.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE l.evt_block_time >= '{{looksrare_v2_start_date}}'
    {% endif %}

    UNION ALL

    SELECT l.evt_block_time AS block_time
    , l.evt_block_number AS block_number
    , 'Buy' AS trade_category
    , l.feeAmounts[0]+l.feeAmounts[1]+l.feeAmounts[2] AS amount_raw
    , l.feeRecipients[0] AS seller
    , l.bidUser AS buyer
    , l.collection AS nft_contract_address
    , element_at(l.amounts, 1) AS number_of_items
    , l.currency
    , l.itemIds[0] AS token_id
    , l.contract_address AS project_contract_address
    , l.evt_tx_hash AS tx_hash
    , l.evt_index
    , l.feeAmounts[1] AS royalty_fee_amount_raw
    , l.feeAmounts[2] AS platform_fee_amount_raw
    , CASE WHEN l.feeRecipients[1]!='0x0000000000000000000000000000000000000000' THEN l.feeRecipients[1] END AS royalty_fee_receive_address
    , get_json_object(l.nonceInvalidationParameters, '$.orderHash') AS order_hash
    FROM {{ source('looksrare_v2_ethereum','LooksRareProtocol_evt_TakerBid') }} l
    {% if is_incremental() %}
    WHERE l.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE l.evt_block_time >= '{{looksrare_v2_start_date}}'
    {% endif %}
    )

SELECT 'ethereum' AS blockchain
, 'looksrare' AS project
, 'v2' AS version
, lt.block_time
, date_trunc('day', lt.block_time) AS block_date
, lt.block_number
, lt.trade_category
, 'Trade' AS evt_type
, lt.seller
, lt.buyer
, CAST(lt.amount_raw as DECIMAL(38,0)) as amount_raw
, CASE WHEN lt.currency='0x0000000000000000000000000000000000000000' THEN lt.amount_raw/POWER(10, 18)
    ELSE lt.amount_raw/POWER(10, pu.decimals)
    END AS amount_original
, CASE WHEN lt.currency='0x0000000000000000000000000000000000000000' THEN pu.price*lt.amount_raw/POWER(10, 18)
    ELSE pu.price*lt.amount_raw/POWER(10, pu.decimals)
    END AS amount_usd
, nft.standard AS token_standard
, CASE WHEN lt.currency='0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE pu.symbol END AS currency_symbol
, lt.nft_contract_address
, COALESCE(agg.name, agg_m.aggregator_name) AS aggregator_name
, agg.contract_address AS aggregator_address
, nft.name AS collection
, CASE WHEN lt.number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type
, lt.number_of_items
, CASE WHEN lt.currency='0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE lt.currency
    END AS currency_contract
, lt.token_id
, lt.project_contract_address
, lt.tx_hash
, lt.evt_index
, et.from AS tx_from
, et.to AS tx_to
, lt.platform_fee_amount_raw
, CASE WHEN lt.currency = '0x0000000000000000000000000000000000000000' THEN lt.platform_fee_amount_raw/POWER(10, 18)
    ELSE lt.platform_fee_amount_raw/POWER(10, currency_tok.decimals)
    END AS platform_fee_amount
, CASE WHEN lt.currency = '0x0000000000000000000000000000000000000000' THEN pu.price*lt.platform_fee_amount_raw/POWER(10, 18)
    ELSE pu.price*lt.platform_fee_amount_raw/POWER(10, currency_tok.decimals)
    END AS platform_fee_amount_usd
, 100*lt.platform_fee_amount_raw/lt.amount_raw AS platform_fee_percentage
, lt.royalty_fee_amount_raw
, CASE WHEN lt.currency = '0x0000000000000000000000000000000000000000' THEN lt.royalty_fee_amount_raw/POWER(10, 18)
    ELSE lt.royalty_fee_amount_raw/POWER(10, currency_tok.decimals)
    END AS royalty_fee_amount
, CASE WHEN lt.currency = '0x0000000000000000000000000000000000000000' THEN pu.price*lt.royalty_fee_amount_raw/POWER(10, 18)
    ELSE pu.price*lt.royalty_fee_amount_raw/POWER(10, currency_tok.decimals)
    END AS royalty_fee_amount_usd
, CASE WHEN lt.royalty_fee_receive_address IS NOT NULL AND lt.currency='0x0000000000000000000000000000000000000000' THEN 'ETH'
    WHEN lt.royalty_fee_receive_address IS NOT NULL THEN pu.symbol
    END AS royalty_fee_currency_symbol
, lt.royalty_fee_receive_address
, 100*lt.royalty_fee_amount_raw/lt.amount_raw AS royalty_fee_percentage
, 'ethereum-looksrare-v2-' || order_hash AS unique_trade_id
FROM looksrare_v2_trades lt
INNER JOIN {{ source('ethereum','transactions') }} et ON et.block_number=lt.block_number
    AND et.hash=lt.tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND et.block_time >= '{{looksrare_v2_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft ON lt.nft_contract_address=nft.contract_address
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_m
    ON RIGHT(et.data, agg_m.hash_marker_size) = agg_m.hash_marker
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} currency_tok ON lt.currency=currency_tok.contract_address
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain='ethereum'
    AND pu.minute=date_trunc('minute', lt.block_time)
    AND (lt.currency=pu.contract_address
        OR (lt.currency='0x0000000000000000000000000000000000000000' AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute >= '{{looksrare_v2_start_date}}'
    {% endif %}
