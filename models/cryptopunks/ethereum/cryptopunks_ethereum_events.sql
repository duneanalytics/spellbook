{{ config(
        alias ='events',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

WITH buys AS (
  SELECT
    `type`,
    punkIndex,
    CASE
      WHEN (buy_value = 0 AND `to` = '0x0000000000000000000000000000000000000000') THEN lag(bid_value) OVER (
        PARTITION BY punkIndex
        ORDER BY
          evt_block_number ASC,
          evt_index ASC
      )
      ELSE buy_value
    END AS `enhanced_buy_value`,
    `from`,
    `bid_from`,
    CASE
      WHEN (buy_value = 0 AND `to` = '0x0000000000000000000000000000000000000000') THEN lag(`bid_from`) OVER (
        PARTITION BY punkIndex
        ORDER BY
          evt_block_number ASC,
          evt_index ASC
      )
      ELSE `to`
    END AS `enhanced_to`,
    evt_block_time,
    evt_block_number,
    evt_index,
    evt_tx_hash,
    contract_address,
    CASE WHEN (buy_value = 0 AND `to` = '0x0000000000000000000000000000000000000000') THEN "Offer Accepted" ELSE "Buy" END AS trade_category
  FROM
    (
      SELECT
        "PunkBought" AS `type`,
        punkIndex,
        value AS buy_value,
        NULL AS bid_value,
        toAddress as `to`,
        fromAddress AS `from`,
        NULL AS `bid_from`,
        evt_block_number,
        evt_index,
        evt_block_time,
        evt_tx_hash,
        contract_address
      FROM
        {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }}
      UNION ALL
        -- `from` is address that placed the bid
      SELECT
        "PunkBidEntered" AS `type`,
        punkIndex,
        NULL AS buy_value,
        value AS bid_value,
        NULL,
        NULL AS `from`,
        fromAddress AS `bid_from`,
        evt_block_number,
        evt_index,
        evt_block_time,
        evt_tx_hash,
        contract_address
      FROM
        {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidEntered') }}
    )
)
SELECT
    "ethereum" as blockchain,
    "cryptopunks" as project,
    "v1" as `version`,
    buys.evt_block_time AS block_time,
    buys.punkIndex as token_id,
    "CryptoPunks" AS `collection`,
    buys.enhanced_buy_value / power(10,18) * p.price AS amount_usd,
    "erc20" AS token_standard,
    NULL::string AS trade_type,
    NULL::string AS number_of_items,
    buys.trade_category,
    buys.from AS seller,
    CASE WHEN buys.`enhanced_to`= agg.contract_address THEN NULL::string --erc.to
      ELSE buys.`enhanced_to` END AS buyer,
    "Trade" as evt_type,
    buys.enhanced_buy_value / power(10,18) AS amount_original,
    buys.enhanced_buy_value AS amount_raw,
    "ETH" AS currency_symbol,
    "0x0000000000000000000000000000000000000000" AS currency_contract,
    "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" AS nft_contract_address,
    "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" AS project_contract_address,
    agg.name as aggregator_name,
    agg.contract_address as aggregator_address,
    buys.evt_block_number AS block_number,
    buys.evt_tx_hash AS tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    0::double AS platform_fee_amount_raw,
    0::double AS platform_fee_amount,
    0::double AS platform_fee_amount_usd,
    0::double AS platform_fee_percentage,
    0::double AS royalty_fee_amount_raw,
    0::double AS royalty_fee_amount,
    0::double AS royalty_fee_amount_usd,
    0::double  AS royalty_fee_percentage,
    NULL::string as royalty_fee_receive_address,
    NULL::string as royalty_fee_currency_symbol,
    "cryptopunks" || '-' || buys.evt_tx_hash || '-' || buys.punkIndex || '-' ||  buys.from || '-' || buys.evt_index || '-' || "" as unique_trade_id
FROM buys
INNER JOIN {{ source('ethereum','transactions') }} tx ON buys.evt_tx_hash = tx.hash
{% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address = tx.to
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', buys.evt_block_time)
    AND p.contract_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    AND p.blockchain = "ethereum"
{% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
{% endif %}
WHERE buys.type = "PunkBought"
