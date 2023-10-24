{{ config(
tags=['prod_exclude'],
        alias = 'erc721_noncompliant'
) 
}}

/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/

WITH 

multiple_owners as (
    select 
        blockchain,
        token_address,
        tokenId,
        count(wallet_address) as holder_count --should always be 1
    from {{ ref('transfers_ethereum_erc721_rolling_day') }}
    WHERE recency_index = 1
    AND amount = 1
    group by blockchain, token_address, tokenId
    having count(wallet_address) > 1
)

select distinct token_address as token_address FROM multiple_owners