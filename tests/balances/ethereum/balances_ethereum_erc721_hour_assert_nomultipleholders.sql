-- Check for multiple holders

select 
    blockchain,
    hour,
    token_address,
    tokenId,
    count(wallet_address) as holder_count --should always be 1
from {{ ref('balances_ethereum_erc721_hour') }}
where hour >= now() - interval '12 hours'
group by blockchain, hour, token_address, tokenId
having count(wallet_address) > 1

--asserting that the above query returns no rows
--checking if there are tokens with multiple holders in the last 2 hours
--this cannot be the case. If this returns any rows, the test fails
