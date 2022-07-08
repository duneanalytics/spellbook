CREATE OR REPLACE VIEW bancornetwork.view_add_liquidity_pool AS
WITH liquidity_pool_added AS (
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v3_evt_LiquidityPoolAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v4_evt_LiquidityPoolAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v5_evt_LiquidityPoolAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v6_evt_LiquidityPoolAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v7_evt_LiquidityPoolAdded"
)
SELECT r."exchange_token",
       r."exchange_token_symbol",
       r."exchange_token_decimals",
       r."base_token",
       r."base_token_symbol",
       r."base_token_decimals",
       "_liquidityPool" AS liquidity_pool,
       q.contract_address,
       q.evt_tx_hash AS tx_hash,
       q.evt_block_time AS block_time
FROM liquidity_pool_added q
LEFT JOIN
  (SELECT DISTINCT ON (s.smart_token) s.convertible_token AS "exchange_token",
                      t2.symbol AS "exchange_token_symbol",
                      t2.decimals AS "exchange_token_decimals",
                      p.convertible_token AS "base_token",
                      p.symbol AS "base_token_symbol",
                      p.decimals AS "base_token_decimals",
                      s.smart_token
   FROM bancornetwork.view_add_convertible_token s
   INNER JOIN
     (SELECT *
      FROM bancornetwork.view_add_convertible_token
     ) p ON s.smart_token = p.smart_token
   AND s.convertible_token != p.convertible_token
   AND (s.convertible_token NOT IN ('\x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c',
                                      '\x309627af60f0926daa6041b8279484312f2bf060')
        OR (s.convertible_token = '\x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c'
            AND p.convertible_token = '\x309627af60f0926daa6041b8279484312f2bf060'))
   LEFT JOIN erc20.tokens t2 ON s.convertible_token = t2.contract_address) r ON q."_liquidityPool" = r.smart_token
;

-- use BNT and USDB as base token for convenience
-- add exception for the case USDB <> BNT
