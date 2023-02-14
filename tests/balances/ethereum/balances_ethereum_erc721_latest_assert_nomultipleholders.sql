select 
    blockchain,
    token_address,
    tokenId,
    count(wallet_address)
from {{ ref('balances_ethereum_erc721_day') }}
group by blockchain, token_address, tokenId
having count(wallet_address) > 1