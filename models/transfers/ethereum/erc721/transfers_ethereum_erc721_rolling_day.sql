{{ config(
        alias = alias('erc721_rolling_day'))
}}

        select
            'ethereum' as blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            cast(current_timestamp as timestamp) as updated_at,
            sum(amount) over (partition by wallet_address, token_address, tokenId order by day) as amount, 
            row_number() over (partition by wallet_address, token_address, tokenId order by day desc) as recency_index
        from {{ ref('transfers_ethereum_erc721_agg_day') }}
;