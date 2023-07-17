select 
    blockchain,
    token_address,
    tokenId,
    count(wallet_address) as holder_count --should always be 1
from {{ ref('balances_ethereum_erc721_latest') }}
group by blockchain, token_address, tokenId
having count(wallet_address) > 1