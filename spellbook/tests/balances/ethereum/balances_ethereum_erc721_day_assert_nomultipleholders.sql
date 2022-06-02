-- Check for multiple holders

select blockchain,
    day,
    wallet_address,
    token_address,
    tokenId,
    count(*)
from {{ ref('balances_ethereum_erc721_day') }}
group by blockchain, day, wallet_address, token_address, tokenId
having count(*) > 1
where hour >= now() - interval '4 days'