{{ config(
        alias='erc721_latest',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}

with
    minutes as (
        select
            max(number) || '_' || 0 as evt_block_number_index,  
            date_trunc('minute', time) as minute
        from {{ source('ethereum', 'blocks') }}
        group by 2

    )

SELECT
    'ethereum' as blockchain,
    d.minute,
    b.evt_block_number_index,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.num_tokens,
    nft_tokens.name as collection
FROM minutes d
INNER JOIN {{ ref('transfers_ethereum_erc721_agg') }} b ON (b.evt_block_number_index <= d.evt_block_number_index AND d.evt_block_number_index < b.next_evt)
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON (nft_tokens.contract_address = b.token_address)
where num_tokens = 1
and d.minute = date_trunc('minute', now()) --as recent as the database pipeline allows as we are using the db time as base

--only here can we drop the prior owners with "num_token=1" to have a continuos chain of ownership for a specific NFT