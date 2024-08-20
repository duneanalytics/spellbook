{{ config(
        schema='prices',
        alias = 'usd_native'
        )
}}
-- this is a TEMPORARY spell that should be incorporated in the general prices models.
-- more discussion here: https://github.com/duneanalytics/spellbook/issues/6577

WITH blockchains as
(
select
    evm.blockchain
    ,evm.native_token_symbol as symbol
    ,{{var('ETH_ERC20_ADDRESS')}} as contract_address -- 0x00..00
    ,18 as decimals
from {{source('evms','info')}} evm
inner join {{source('prices_native','tokens')}} p
on native_token_symbol = p.symbol
)

SELECT
  b.blockchain
, b.contract_address
, b.decimals
, b.symbol
, p.minute
, p.price
FROM {{ source('prices', 'usd') }} p
INNER JOIN blockchains b
ON b.symbol = p.symbol
and p.blockchain is null
