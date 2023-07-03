{{ config(
    schema = 'magiceden_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

{% set nft_start_date = '2022-03-16' %}
{% set magic_eden_nonce = '10013141590000000000000000000000000000' %}

WITH trades AS (
    SELECT CASE when direction = 0 THEN 'buy' ELSE 'sell' END AS trade_category,
          evt_block_time,
          evt_block_number,
          evt_tx_hash,
          contract_address,
          evt_index,
          'Trade' AS evt_type,
          CASE when direction = 0 THEN taker ELSE maker END AS buyer,
          CASE when direction = 0 THEN maker ELSE taker END AS seller,
          erc721Token AS nft_contract_address,
          erc721TokenId AS token_id,
          1 AS number_of_items,
          'erc721' AS token_standard,
          CASE
               WHEN erc20Token in ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
               THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
               ELSE erc20Token
          END AS currency_contract,
          erc20TokenAmount AS amount_raw,
          erc20Token as original_erc20_token
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC721OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    SELECT CASE when direction = 0 THEN 'buy' ELSE 'sell' END AS trade_category,
          evt_block_time,
          evt_block_number,
          evt_tx_hash,
          contract_address,
          evt_index,
          'Trade' AS evt_type,
          CASE when direction = 0 THEN taker ELSE maker END AS buyer,
          CASE when direction = 0 THEN maker ELSE taker END AS seller,
          erc1155Token AS nft_contract_address,
          erc1155TokenId AS token_id,
          erc1155FillAmount AS number_of_items,
          'erc1155' AS token_standard,
          CASE
               WHEN erc20Token in ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
               THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
               ELSE erc20Token
          END AS currency_contract,
          erc20FillAmount AS amount_raw,
          erc20Token as original_erc20_token
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC1155OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

-- There are orders purchase multiple tokens using native MATIC as currency. There is no good way to track total payment for each token using traces table.
-- So here we find the "order_amount_percentage" and then use it to calculate total amount_raw, platform_fee_amount_raw and royalty_fee_amount_raw
-- Max platform fee percentage is set to 2% and max royalty fee percentage is set to 0.5%.
-- This can be updated if a better solution is found.

native_order_summary AS (
    SELECT evt_block_number,
        evt_tx_hash,
        sum(cast(amount_raw AS decimal(38, 0))) AS order_amount_raw
    FROM trades
    WHERE original_erc20_token IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
    GROUP BY 1, 2
),

-- For ERC1155 order, there are returned amount to buyer. This need be excluded when calculat total transaction amount
-- Sample: https://polygonscan.com/tx/0xf5cdac86703bd70ad0f5e70be6faf77e1187badbef6edd69dd9c3e8346584832
native_order_return_amount AS (
    SELECT evt_block_number,
        evt_tx_hash,
        sum(return_amount_raw) AS return_amount_raw
    FROM (
        SELECT DISTINCT t.evt_block_number,
            t.evt_tx_hash,
            cast(tc.value as decimal(38, 0)) as return_amount_raw
        FROM trades t
        INNER JOIN {{ source('polygon', 'traces') }} tc ON t.evt_block_number = tc.block_number
            AND t.evt_tx_hash = tc.tx_hash
            {% if not is_incremental() %}
            AND tc.block_time >= '{{nft_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tc.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        WHERE tc.`to` = t.buyer
    ) t
    GROUP BY 1, 2
),

native_order_total_amount AS (
    SELECT o.evt_block_number,
        o.evt_tx_hash,
        o.order_amount_raw,
        cast(t.value AS decimal(38, 0)) - coalesce(r.return_amount_raw, 0) AS transaction_amount_raw,
        o.order_amount_raw / (cast(t.value AS decimal(38, 0)) - coalesce(r.return_amount_raw, 0)) AS order_amount_percentage
    FROM native_order_summary o
    INNER JOIN {{ source('polygon', 'transactions') }} t ON o.evt_block_number = t.block_number
        AND o.evt_tx_hash = t.hash
        {% if not is_incremental() %}
        AND t.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN native_order_return_amount r ON o.evt_block_number = r.evt_block_number
        AND o.evt_tx_hash = r.evt_tx_hash
),

native_trade_amount_summary AS (
    SELECT t.evt_block_number,
        t.evt_tx_hash,
        t.token_id,
        t.currency_contract,
        cast(t.amount_raw AS decimal(38, 0)) / o.order_amount_percentage as amount_raw,
        -- When the payment to seller is less than 98%, we hard code platform fee to 2%
        (CASE WHEN 1.0 - o.order_amount_percentage >= 0.02 THEN cast(t.amount_raw AS decimal(38, 0)) / o.order_amount_percentage * 0.02
            ELSE cast(t.amount_raw AS decimal(38, 0)) / o.order_amount_percentage * (1.0 - o.order_amount_percentage)
        END) AS platform_fee_amount_raw,
        -- When there is still remaining exclude payment to seller and platform fee, we set royalty fee
        (CASE WHEN 1.0 - o.order_amount_percentage >= 0.025 THEN cast(t.amount_raw AS decimal(38, 0)) / o.order_amount_percentage * 0.005
            WHEN 1.0 - o.order_amount_percentage >= 0.02 THEN cast(t.amount_raw AS decimal(38, 0)) / o.order_amount_percentage - cast(t.amount_raw AS decimal(38, 0)) - cast(t.amount_raw AS decimal(38, 0)) / o.order_amount_percentage * 0.02
            ELSE 0
        END) AS royalty_fee_amount_raw
    FROM trades t
    INNER JOIN native_order_total_amount o ON t.evt_block_number = o.evt_block_number
        AND o.evt_tx_hash = t.evt_tx_hash
    WHERE t.original_erc20_token IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
),

erc20_trade_amount_detail as (
    SELECT e.evt_block_number,
        e.evt_tx_hash,
        t.token_id,
        t.currency_contract,
        CAST(e.value as double) AS amount_raw,
        row_number() OVER (PARTITION BY e.evt_tx_hash, e.contract_address ORDER BY e.evt_index) AS item_index
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN {{ source('polygon', 'transactions') }} tx ON e.evt_block_number = tx.block_number
        AND e.evt_tx_hash = tx.hash
        {% if not is_incremental() %}
        AND tx.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    INNER JOIN trades t ON e.evt_block_number = t.evt_block_number and t.currency_contract = e.contract_address
        AND e.evt_tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE t.original_erc20_token NOT IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
        AND e.`to` <> tx.`to` -- exclude transfer to contract, which is just a temp transfer
),

erc20_trade_amount_summary AS (
    SELECT evt_block_number,
        evt_tx_hash,
        token_id,
        currency_contract,
        sum(amount_raw) AS amount_raw,
        sum(case when item_index = 2 then amount_raw else 0 end) AS platform_fee_amount_raw,
        sum(case when item_index = 3 then amount_raw else 0 end) AS royalty_fee_amount_raw
    FROM erc20_trade_amount_detail
    GROUP BY 1, 2, 3, 4
),

trade_amount_summary AS (
    SELECT evt_block_number,
        evt_tx_hash,
        token_id,
        currency_contract,
        amount_raw,
        platform_fee_amount_raw,
        royalty_fee_amount_raw
    FROM native_trade_amount_summary

    UNION ALL

    SELECT evt_block_number,
        evt_tx_hash,
        token_id,
        currency_contract,
        amount_raw,
        platform_fee_amount_raw,
        royalty_fee_amount_raw
    FROM erc20_trade_amount_summary
)

SELECT
  'polygon' AS blockchain,
  'magiceden' AS project,
  'v1' AS version,
  a.evt_tx_hash AS tx_hash,
  date_trunc('day', a.evt_block_time) AS block_date,
  a.evt_block_time AS block_time,
  a.evt_block_number AS block_number,
  s.amount_raw / power(10, erc.decimals) * p.price AS amount_usd,
  s.amount_raw / power(10, erc.decimals) AS amount_original,
  CAST(s.amount_raw as decimal(38,0)) AS amount_raw,
  CASE WHEN erc.symbol = 'WMATIC' THEN 'MATIC' ELSE erc.symbol END AS currency_symbol,
  a.currency_contract,
  a.token_id,
  token_standard,
  a.contract_address AS project_contract_address,
  evt_type,
  CAST(NULL AS string) AS collection,
  CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
  CAST(number_of_items AS decimal(38,0)) AS number_of_items,
  CAST(NULL AS string) AS trade_category,
  buyer,
  seller,
  nft_contract_address,
  agg.name AS aggregator_name,
  agg.contract_address AS aggregator_address,
  t.`from` AS tx_from,
  t.`to` AS tx_to,
  s.platform_fee_amount_raw,
  CAST(s.platform_fee_amount_raw / power(10, erc.decimals) AS double) AS platform_fee_amount,
  CAST(s.platform_fee_amount_raw / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
  CAST(s.platform_fee_amount_raw  / s.amount_raw * 100 as double) as platform_fee_percentage,
  CAST(s.royalty_fee_amount_raw AS double) AS royalty_fee_amount_raw,
  CAST(s.royalty_fee_amount_raw / power(10, erc.decimals) AS double) AS royalty_fee_amount,
  CAST(s.royalty_fee_amount_raw / power(10, erc.decimals) * p.price AS double) AS royalty_fee_amount_usd,
  CAST(s.royalty_fee_amount_raw / s.amount_raw * 100 AS double) AS royalty_fee_percentage,
  CAST(NULL AS varchar(5)) AS royalty_fee_receive_address,
  CAST(NULL AS string) AS royalty_fee_currency_symbol,
  a.evt_tx_hash || '-' || a.evt_type  || '-' || a.evt_index ||  '-' || a.token_id || '-' || cast(a.number_of_items as string) AS unique_trade_id
FROM trades a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN trade_amount_summary s ON a.evt_block_number = s.evt_block_number
    AND a.evt_tx_hash = s.evt_tx_hash
    AND a.token_id = s.token_id
    AND a.currency_contract = s.currency_contract
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN {{ source('prices', 'usd') }} p ON p.contract_address = a.currency_contract
    AND p.minute = date_trunc('minute', a.evt_block_time)
    {% if not is_incremental() %}
    AND p.minute >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
