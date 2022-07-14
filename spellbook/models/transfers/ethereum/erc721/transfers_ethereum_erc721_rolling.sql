{{ config(
        alias ='erc721_rolling_day')
}}

        select
            'ethereum' as blockchain,
            evt_block_time,
            wallet_address,
            token_address,
            tokenId,
            num_tokens,
            current_timestamp() as updated_at,
            lead(evt_block_time, 1, now()) OVER (PARTITION BY wallet_address, token_address, tokenId ORDER BY evt_block_time ASC) AS next_evt
        from {{ ref('transfers_ethereum_erc721_agg') }}

--this will look for the next event specific to that wallet and a specific NFT, used to carry forward state in the balance table