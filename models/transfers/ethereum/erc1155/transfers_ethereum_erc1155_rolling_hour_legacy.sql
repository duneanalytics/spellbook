{{ config(
	tags=['legacy'],
	
        alias = alias('erc1155_rolling_hour', legacy_model=True)
        )
}}

        select
            'ethereum' as blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            cast(current_timestamp as timestamp) as updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by hour desc) as recency_index,
            sum(amount) over (
                partition by token_address, tokenId, wallet_address order by hour
            ) as amount
        from {{ ref('transfers_ethereum_erc1155_agg_hour_legacy') }}