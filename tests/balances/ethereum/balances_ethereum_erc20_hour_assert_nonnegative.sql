                                    -- Check for negative balances
-- Some balances are very small negative numbers due to loss of precision from large ints

select *
from {{ ref('balances_ethereum_erc20_hour') }} bal
where round(amount/power(10, 18), 6) < 0
-- limiting to a selection of tokens because we haven't filtered out all non-compliant tokens
and token_address in (0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9, 0x6B175474E89094C44Da98b954EedeAC495271d0F, 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984, 0x514910771AF9Ca656af840dff83E8264EcF986CA)
and wallet_address != 0x0000000000000000000000000000000000000000
and bal.block_hour > now() - interval '2' Day

