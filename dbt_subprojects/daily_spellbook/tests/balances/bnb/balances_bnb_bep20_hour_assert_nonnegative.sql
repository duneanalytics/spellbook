-- copied over from balances ethereum tests 

select amount
from {{ ref('balances_bnb_bep20_hour') }} bal
where bal.block_hour > now() - interval '1' Day
and round(amount/power(10, 18), 6) < 0
and symbol in ('AAVE', 'DAI', 'UNI', 'LINK')

