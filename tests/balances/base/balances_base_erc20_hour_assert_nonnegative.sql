-- Check for negative balances
-- Some balances are very small negative numbers due to loss of precision from large ints

select amount
from {{ ref('balances_base_erc20_hour') }} bal

where round(amount/power(10, 18), 6) < 0

-- limiting to a selection of tokens because we haven't filtered out all non-compliant tokens
and bal.token_address in ( 0x50c5725949a6f0c72e6c4a641f24049a917db0cb,
                        0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,
                         0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913) --'DAI', 'USDbc', 'USDC'
and bal.block_hour > now() - interval '2' day


