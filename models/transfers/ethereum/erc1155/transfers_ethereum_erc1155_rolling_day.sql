{{ config(
        alias = alias('erc1155_rolling_day')
        )
}}

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