{{ config(
        alias ='erc721_rolling_hour')
}}

        select
            'ethereum' as blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() as updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by hour desc) as recency_index
        from {{ ref('transfers_ethereum_erc721_agg_hour') }}