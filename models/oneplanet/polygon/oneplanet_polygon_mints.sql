 {{
  config(
      schema = 'oneplanet_polygon',
      alias='mints',
      partition_by = ['block_date'],
      materialized = 'incremental',
      file_format = 'delta',
      incremental_strategy = 'merge',
      unique_key = ['block_time', 'unique_trade_id'],
      post_hook='{{ expose_spells(\'["polygon"]\',
                              "project",
                              "oneplanet",
                              \'["springzh"]\') }}')
}}

{% set nft_start_date = "2022-12-30" %}

WITH contract_list as (
    SELECT distinct token_contract_address
    FROM {{ ref('oneplanet_polygon_base_pairs') }}
),

mints as (
    SELECT 'mint' AS trade_category,
        block_time,
        block_number,
        tx_hash,
        CAST(NULL AS string) AS contract_address,
        evt_index,
        'Mint' AS evt_type,
        `to` AS buyer,
        CAST(NULL AS string) AS seller,
        contract_address AS nft_contract_address,
        token_id,
        amount AS number_of_items,
        token_standard,
        '0x0000000000000000000000000000000000001010' AS currency_contract,
        'MATIC' as currency_symbol,
        CAST(0 as DECIMAL(38,0)) AS amount_raw
    FROM {{ ref('nft_polygon_transfers') }}
    WHERE contract_address IN ( SELECT token_contract_address FROM contract_list )
        AND `from` = '0x0000000000000000000000000000000000000000'   -- mint
        {% if not is_incremental() %}
        AND block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT
    'polygon' AS blockchain,
    'oneplanet' AS project,
    'v1' AS version,
    date_trunc('day', a.block_time) as block_date,
    a.block_time,
    token_id,
    CAST(NULL AS string) AS collection,
    CAST(0 as DECIMAL(38,0)) AS amount_usd,
    token_standard,
    CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    number_of_items,
    a.trade_category,
    evt_type,
    a.seller,
    a.buyer,
    CAST(0 as DECIMAL(38,0)) AS amount_original,
    amount_raw,
    a.currency_symbol,
    a.currency_contract,
    a.nft_contract_address,
    coalesce(a.contract_address, t.`to`) AS project_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    a.block_number,
    a.tx_hash,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    'OnePlanet-' || a.tx_hash || '-' || a.evt_type || '-' || a.evt_index || '-' || a.token_id  AS unique_trade_id
FROM mints a
INNER JOIN {{ source('polygon','transactions') }} t ON a.block_number = t.block_number
    AND a.tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
