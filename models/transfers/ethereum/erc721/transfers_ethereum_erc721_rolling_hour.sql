{{ config(
        alias = 'erc721_rolling_hour')
}}
/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/
        select
            'ethereum' as blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            NOW() as updated_at,
            sum(amount) over (partition by wallet_address, token_address, tokenId order by hour) as amount, 
            row_number() over (partition by wallet_address, token_address, tokenId order by hour desc) as recency_index
        from {{ ref('transfers_ethereum_erc721_agg_hour') }}
;