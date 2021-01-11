CREATE OR REPLACE VIEW compound.view_liquidate_borrow AS
SELECT CASE
           WHEN t.symbol = 'WETH' THEN 'ETH'
           ELSE t.symbol
       END AS token_symbol,
       repay."repayAmount"::double precision / (10::double precision ^ t.decimals::double precision) AS repay_amount,
       repay."repayAmount"::double precision / (10::double precision ^ t.decimals::double precision) * p.price AS repay_amount_usd,
       c.symbol AS ctoken_symbol,
       events."seizeTokens" / (10::numeric ^ c.decimals) AS seize_ctokens,
       events.liquidator,
       events.borrower,
       c_repay.underlying_token_address AS underlying_token,
       events.evt_tx_hash AS tx_hash,
       repay.evt_block_time  AS block_time
FROM (
    SELECT "cErc20_evt_LiquidateBorrow".liquidator,
               "cErc20_evt_LiquidateBorrow".borrower,
               "cErc20_evt_LiquidateBorrow"."repayAmount",
               "cErc20_evt_LiquidateBorrow"."cTokenCollateral",
               "cErc20_evt_LiquidateBorrow"."seizeTokens",
               "cErc20_evt_LiquidateBorrow".contract_address,
               "cErc20_evt_LiquidateBorrow".evt_tx_hash,
               "cErc20_evt_LiquidateBorrow".evt_block_time
        FROM compound_v2."cErc20_evt_LiquidateBorrow"
        UNION 
        SELECT "cEther_evt_LiquidateBorrow".liquidator,
               "cEther_evt_LiquidateBorrow".borrower,
               "cEther_evt_LiquidateBorrow"."repayAmount",
               "cEther_evt_LiquidateBorrow"."cTokenCollateral",
               "cEther_evt_LiquidateBorrow"."seizeTokens",
               "cEther_evt_LiquidateBorrow".contract_address,
               "cEther_evt_LiquidateBorrow".evt_tx_hash,
               "cEther_evt_LiquidateBorrow".evt_block_time 
        FROM compound_v2."cEther_evt_LiquidateBorrow" 
        UNION 
        SELECT "CErc20Delegator_evt_LiquidateBorrow".liquidator,
               "CErc20Delegator_evt_LiquidateBorrow".borrower,
               "CErc20Delegator_evt_LiquidateBorrow"."repayAmount",
               "CErc20Delegator_evt_LiquidateBorrow"."cTokenCollateral",
               "CErc20Delegator_evt_LiquidateBorrow"."seizeTokens",
               "CErc20Delegator_evt_LiquidateBorrow".contract_address,
               "CErc20Delegator_evt_LiquidateBorrow".evt_tx_hash,
               "CErc20Delegator_evt_LiquidateBorrow".evt_block_time 
        FROM compound_v2."CErc20Delegator_evt_LiquidateBorrow"
) events
LEFT JOIN (
    SELECT "cErc20_evt_RepayBorrow".payer,
           "cErc20_evt_RepayBorrow".borrower,
           "cErc20_evt_RepayBorrow"."repayAmount",
           "cErc20_evt_RepayBorrow"."accountBorrows",
           "cErc20_evt_RepayBorrow"."totalBorrows",
           "cErc20_evt_RepayBorrow".contract_address,
           "cErc20_evt_RepayBorrow".evt_tx_hash,
           "cErc20_evt_RepayBorrow".evt_block_time
    FROM compound_v2."cErc20_evt_RepayBorrow"
    UNION
    SELECT "cEther_evt_RepayBorrow".payer,
           "cEther_evt_RepayBorrow".borrower,
           "cEther_evt_RepayBorrow"."repayAmount",
           "cEther_evt_RepayBorrow"."accountBorrows",
           "cEther_evt_RepayBorrow"."totalBorrows",
           "cEther_evt_RepayBorrow".contract_address,
           "cEther_evt_RepayBorrow".evt_tx_hash,
           "cEther_evt_RepayBorrow".evt_block_time
    FROM compound_v2."cEther_evt_RepayBorrow"
    UNION
    SELECT "CErc20Delegator_evt_RepayBorrow".payer,
           "CErc20Delegator_evt_RepayBorrow".borrower,
           "CErc20Delegator_evt_RepayBorrow"."repayAmount",
           "CErc20Delegator_evt_RepayBorrow"."accountBorrows",
           "CErc20Delegator_evt_RepayBorrow"."totalBorrows",
           "CErc20Delegator_evt_RepayBorrow".contract_address,
           "CErc20Delegator_evt_RepayBorrow".evt_tx_hash,
           "CErc20Delegator_evt_RepayBorrow".evt_block_time
    FROM compound_v2."CErc20Delegator_evt_RepayBorrow"
) repay ON events.evt_tx_hash = repay.evt_tx_hash
LEFT JOIN compound.view_ctokens c ON events."cTokenCollateral" = c.contract_address
LEFT JOIN compound.view_ctokens c_repay ON repay.contract_address = c_repay.contract_address
LEFT JOIN erc20.tokens t ON c_repay.underlying_token_address = t.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', events.evt_block_time)
AND p.contract_address = c_repay.underlying_token_address
;
