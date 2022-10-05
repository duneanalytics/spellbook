-- Check for multiple holders

select blockchain,
    day,
    token_address,
    tokenId,
    count(*)
from {{ ref('balances_ethereum_erc721_day') }}
where day >= now() - interval '2 days'
group by blockchain, day, token_address, tokenId
having count(*) > 1
