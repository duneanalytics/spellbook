CREATE OR REPLACE FUNCTION zerion.insert_oneinchpre2022(start_ts timestamptz, end_ts timestamptz=NOW()) RETURNS INTEGER
LANGUAGE plpgsql AS $function$
DECLARE r INTEGER;
BEGIN

WITH
  oneinchpre2022 AS (
    SELECT
      dt.block_time,
      dt.tx_from AS trader,
      dt.usd_amount AS usd_volume,
      '1inch' AS protocol,
      oi.tx_hash,
      dt.token_b_amount AS sold_token_amount,
      dt.token_a_amount AS bought_token_amount,
      oi."fromToken" AS sold_token_address,
      oi."toToken" AS bought_token_address,
      dt.token_b_symbol AS sold_token_symbol,
      dt.token_a_symbol AS bought_token_symbol
    FROM
      (
        SELECT
          "toToken",
          "fromToken",
          "minTokensAmount" AS "toAmount",
          "tokensAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v1_call_aggregate"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minTokensAmount" AS "toAmount",
          "tokensAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v2_call_aggregate"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minTokensAmount" AS "toAmount",
          "tokensAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v3_call_aggregate"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minTokensAmount" AS "toAmount",
          "tokensAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v4_call_aggregate"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minTokensAmount" AS "toAmount",
          "tokensAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v5_call_aggregate"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minTokensAmount" AS "toAmount",
          "tokensAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v6_call_aggregate"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minReturnAmount" AS "toAmount",
          "fromTokenAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."exchange_v7_call_swap"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "toToken",
          "fromToken",
          "minReturnAmount" AS "toAmount",
          "fromTokenAmount" AS "fromAmount",
          "call_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "call_block_time" AS "block_time"
        FROM
          oneinch."OneInchExchange_call_swap"
        WHERE
          call_success
          AND referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND call_block_time >= start_ts
          AND call_block_time < end_ts
        UNION ALL
        SELECT
          "dstToken" AS "toToken",
          "srcToken" AS "fromToken",
          "minReturnAmount" AS "toAmount",
          "spentAmount" AS "fromAmount",
          "evt_tx_hash" AS "tx_hash",
          "referrer" AS "ref",
          "evt_block_time" AS "block_time"
        FROM
          oneinch_v2."OneInchExchange_evt_Swapped"
        WHERE
          referrer = '\xD26d332C71Daa06bB24DDED5c3C167961e9eb994'
          AND evt_block_time >= start_ts
          AND evt_block_time < end_ts
        UNION ALL
        SELECT
          "dstToken" AS "toToken",
          "srcToken" AS "fromToken",
          "returnAmount" AS "toAmount",
          "spentAmount" AS "fromAmount",
          "evt_tx_hash" AS "tx_hash",
          substring(
            tr.input
            from
              49 for 20
          ) AS "ref",
          "evt_block_time" AS "block_time"
        FROM
          oneinch_v3."AggregationRouterV3_evt_Swapped" e
          INNER JOIN ethereum.traces tr on tr.tx_hash = e.evt_tx_hash
          AND tr.block_time = e.evt_block_time
          AND substring(
            tr.input
            from
              1 for 4
          ) = '\x05971224'
          AND substring(
            tr.input
            from
              49 for 20
          ) = '\xd26d332c71daa06bb24dded5c3c167961e9eb994'
        WHERE
          e.evt_block_time >= start_ts
          AND e.evt_block_time < end_ts
      ) oi
      INNER JOIN dex.trades dt ON dt.tx_hash = oi.tx_hash
      AND dt.project = '1inch'
      AND dt.block_time = oi.block_time
  )
, rows AS (
    INSERT INTO zerion.trades (
	block_time
    , trader
    , usd_volume
    , protocol
    , tx_hash
    , sold_token_amount
    , bought_token_amount
    , sold_token_address
    , bought_token_address
    , sold_token_symbol
    , bought_token_symbol
    )
    SELECT
    block_time
    , trader
    , usd_volume
    , protocol
    , tx_hash
    , sold_token_amount
    , bought_token_amount
    , sold_token_address
    , bought_token_address
    , sold_token_symbol
    , bought_token_symbol
    FROM oneinchpre2022

    ON CONFLICT DO NOTHING
    RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$;

-- Fill 2020
SELECT zerion.insert_oneinchpre2022(
    '2020-01-01',
    '2021-01-01'
)
;
-- Fill 2021
SELECT zerion.insert_oneinchpre2022(
    '2021-01-01',
    '2022-01-01'
)
;
