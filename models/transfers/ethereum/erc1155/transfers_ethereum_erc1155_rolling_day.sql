{{ config(
        alias = 'erc1155_rolling_day'
        )
}}
/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/
        select
            'ethereum' as blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            NOW() as updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by day desc) as recency_index,
            sum(amount) over (
                partition by token_address, tokenId, wallet_address order by day
            ) as amount
        from {{ ref('transfers_ethereum_erc1155_agg_day') }}