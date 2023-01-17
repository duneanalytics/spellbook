{{ config(
    schema = 'compound_v2_ethereum',
    alias = 'supply',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "compound_v2",
                                    \'["bizzyvinci"]\') }}'
    )
}}

with mints as (
    select
        '2' as version,
        'deposit' as transaction_type,
        asset_symbol as symbol,
        minter as depositor,
        cast(null as varchar(5)) as withdrawn_to,
        cast(null as varchar(5)) as liquidator,
        cast(mintAmount as decimal(38, 0)) / decimals_mantissa as amount,
        cast(mintAmount as decimal(38, 0)) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Mint') }}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Mint') }}
    ) evt_mint
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_mint.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = date_trunc('minute', evt_mint.evt_block_time)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
)


select * from mints
