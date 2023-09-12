-- copied over from balances ethereum tests 
select amount
from {{ ref('balances_polygon_erc20_day') }} bal
where round(amount/power(10, 18), 6) < 0
and symbol in ('AAVE', 'DAI', 'UNI', 'LINK')
and bal.day > now() - interval '2' Day

