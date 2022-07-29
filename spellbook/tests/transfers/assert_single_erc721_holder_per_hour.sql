-- Checks to see if any token is held by more than one wallet

SELECT blockchain,
       hour,
       token_address,
       tokenId,
       count(*) as wallets
FROM {{ ref('balances_ethereum_erc721_hour') }} d
group by 1,2,3,4
having count(*) > 1

