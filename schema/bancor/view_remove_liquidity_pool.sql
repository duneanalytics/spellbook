CREATE OR REPLACE VIEW bancor.view_remove_liquidity_pool AS
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
FROM bancor."BancorConverterRegistry_evt_LiquidityPoolRemoved" q
LEFT JOIN
  (SELECT DISTINCT ON (s."_smartToken") s."_convertibleToken" AS "exchange_token",
                      t2.symbol AS "exchange_token_symbol",
                      t2.decimals AS "exchange_token_decimals",
                      p."_convertibleToken" AS "base_token",
                      p.symbol AS "base_token_symbol",
                      p.decimals AS "base_token_decimals",
                      s."_smartToken" AS smart_token
   FROM bancor."BancorConverterRegistry_evt_ConvertibleTokenRemoved" s
   INNER JOIN
     (SELECT *
      FROM bancor."BancorConverterRegistry_evt_ConvertibleTokenRemoved"
      LEFT JOIN erc20.tokens t1 ON "_convertibleToken" = t1.contract_address) p ON s."_smartToken" = p."_smartToken"
   AND s."_convertibleToken" != p."_convertibleToken"
   AND (s."_convertibleToken" NOT IN ('\x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c',
                                      '\x309627af60f0926daa6041b8279484312f2bf060')
        OR (s."_convertibleToken" = '\x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c'
            AND p."_convertibleToken" = '\x309627af60f0926daa6041b8279484312f2bf060'))
   LEFT JOIN erc20.tokens t2 ON s."_convertibleToken" = t2.contract_address) r ON q."_liquidityPool" = r.smart_token
;

-- use BNT and USDB as base token for convenience
-- add exception for the case USDB <> BNT
