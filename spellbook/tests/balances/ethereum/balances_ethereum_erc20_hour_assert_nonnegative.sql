                                    -- Check for negative balances
-- Some balances are very small negative numbers due to loss of precision from large ints

select amount
from {{ ref('balances_ethereum_erc20_hour') }} bal
where round(amount/power(10, 18), 6) < 0
-- limiting to a selection of tokens because we haven't filtered out all non-compliant tokens
and symbol in ('AAVE', 'DAI', 'UNI', 'LINK')
and bal.hour > now() - interval 2 days

