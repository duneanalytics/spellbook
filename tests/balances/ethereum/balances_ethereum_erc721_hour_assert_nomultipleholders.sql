-- Check for multiple holders

select blockchain,
    hour,
    token_address,
    tokenId,
    count(*)
from {{ ref('balances_ethereum_erc721_hour') }}
where hour >= now() - interval '2 hours'
group by blockchain, hour, token_address, tokenId
having count(*) > 1
