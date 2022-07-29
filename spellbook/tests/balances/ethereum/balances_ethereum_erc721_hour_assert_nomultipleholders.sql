-- Check for multiple holders
-- There should never be more than one wallet in a given hour with the same token

select
    blockchain,
    hour,
    token_address,
    tokenId,
    count(distinct wallet_address) as wallets
from {{ ref('balances_ethereum_erc721_hour') }}
where hour >= now() - interval '2 days'
group by blockchain, hour, token_address, tokenId
having count(distinct wallet_address) > 1
