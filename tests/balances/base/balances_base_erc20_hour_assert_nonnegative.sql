-- Check for negative balances
-- Some balances are very small negative numbers due to loss of precision from large ints
SELECT amount
FROM {{ ref('balances_base_erc20_hour') }} bal
WHERE ROUND(CAST(amount AS DOUBLE) / POWER(10, 18), 6) < 0
-- Limiting to a selection of tokens because we haven't filtered out all non-compliant tokens
AND bal.token_address IN (
    0x50c5725949a6f0c72e6c4a641f24049a917db0cb,
    0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,
    0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
) --'DAI', 'USDbc', 'USDC'
AND bal.block_hour > NOW() - INTERVAL '2' DAY;