-- Check for negative balances
-- Some balances are very small negative numbers due to loss of precision from large ints

select amount
from {{ ref('balances_ethereum_erc20_day') }} bal
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }} nc
    ON bal.token_address = nc.token_address

where round(amount/power(10, 18), 6) < 0

-- limiting to a selection of tokens because we haven't filtered out all non-compliant tokens
and bal.token_address in (0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9, 
                        0x6b175474e89094c44da98b954eedeac495271d0f,
                        0x1f9840a85d5af5bf1d1762f925bdaddc4201f984,
                         0xe41d2489571d322189246dafa5ebde1f4699f498) --'AAVE', 'DAI', 'UNI', 'LINK'
and bal.block_day > now() - interval '2' day
AND nc.token_address IS NULL

