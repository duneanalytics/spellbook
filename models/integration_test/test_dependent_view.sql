{{ config(
        alias = alias('test_dependent_view')
        )
}}


        select
            'ethereum-test' as blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() as updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by day desc) as recency_index,
            sum(amount) over (
                partition by token_address, wallet_address order by day
            ) as amount
        from {{ ref('test_incremental_table') }}
