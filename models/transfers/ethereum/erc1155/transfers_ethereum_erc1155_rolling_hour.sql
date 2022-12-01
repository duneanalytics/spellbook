{{ config(
        alias ='erc1155_rolling_hour'
        )
}}

        select
            'ethereum' as blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() as updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by hour desc) as recency_index,
            sum(amount) over (
                partition by token_address, wallet_address order by hour
            ) as amount
        from {{ ref('transfers_ethereum_erc1155_agg_hour') }}