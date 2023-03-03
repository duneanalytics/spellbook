{{ config(
        alias='erc721_hour',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}

with
    hours as (
        select
            cast(max(number) as double) as evt_block_number_index,  
            date_trunc('hour', time) as hour
        from {{ source('ethereum', 'blocks') }}
        group by 2

    )

SELECT
    'ethereum' as blockchain,
    d.hour,
    b.evt_block_number_index,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.num_tokens,
    nft_tokens.name as collection
FROM hours d
INNER JOIN {{ ref('transfers_ethereum_erc721_agg') }} b ON (b.evt_block_number_index <= d.evt_block_number_index AND d.evt_block_number_index < b.next_evt)
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON (nft_tokens.contract_address = b.token_address)
where num_tokens = 1

-- this will generate the state of the world at the end of each hour
-- this is the table that will be used to generate the hourly balances

;