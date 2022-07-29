-- Check for multiple holders
-- There should never be more than one wallet in a given day with the same token

select
    blockchain,
    day,
    token_address,
    tokenId,
    count(distinct wallet_address) as wallets
from {{ ref('balances_ethereum_erc721_day') }}
where day >= now() - interval '2 days'
group by blockchain, day, token_address, tokenId
having count(distinct wallet_address) > 1
