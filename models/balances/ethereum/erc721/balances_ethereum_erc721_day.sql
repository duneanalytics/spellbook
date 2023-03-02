{{ config(
        alias='erc721_day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}

with
    days as (
        select
            max(number) || '_' || 0 as evt_block_number_evt_index,  
            date_trunc('day', time) as day
        from {{ source('ethereum', 'blocks') }}
        group by 2

    )

SELECT
    'ethereum' as blockchain,
    d.day,
    b.evt_block_number_evt_index,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.num_tokens,
    nft_tokens.name as collection
FROM days d
INNER JOIN {{ ref('transfers_ethereum_erc721_agg') }} b ON (b.evt_block_number_evt_index <= d.evt_block_number_evt_index AND d.evt_block_number_evt_index < b.next_evt)
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON (nft_tokens.contract_address = b.token_address)
where num_tokens = 1

-- this will generate the state of the world at the end of each day
-- this is the table that will be used to generate the daily balances

;