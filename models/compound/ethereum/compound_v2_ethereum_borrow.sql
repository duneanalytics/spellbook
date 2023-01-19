{{ config(
    schema = 'compound_v2_ethereum',
    alias = 'borrow',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "compound_v2",
                                    \'["bizzyvinci"]\') }}'
    )
}}

with borrows as (
    select
        '2' as version,
        'borrow' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        borrower,
        cast(null as varchar(5)) as repayer,
        cast(null as varchar(5)) as liquidator,
        cast(borrowAmount as decimal(38, 0)) / decimals_mantissa as amount,
        cast(borrowAmount as decimal(38, 0)) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Borrow') }}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Borrow') }}
    ) evt_borrow
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_borrow.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = date_trunc('minute', evt_borrow.evt_block_time)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
),
repays as (
    select
        '2' as version,
        'repay' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        borrower,
        payer as repayer,
        cast(null as varchar(5)) as liquidator,
        -cast(repayAmount as decimal(38, 0)) / decimals_mantissa as amount,
        -cast(repayAmount as decimal(38, 0)) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_RepayBorrow') }}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_RepayBorrow') }}
    ) evt_repay
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_repay.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = date_trunc('minute', evt_repay.evt_block_time)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
),
liquidations as (
    select
        '2' as version,
        'borrow_liquidation' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        borrower,
        liquidator as repayer,
        liquidator,
        -cast(repayAmount as decimal(38, 0)) / decimals_mantissa as amount,
        -cast(repayAmount as decimal(38, 0)) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_LiquidateBorrow') }}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_LiquidateBorrow') }}
    ) evt_liquidate
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_liquidate.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = date_trunc('minute', evt_liquidate.evt_block_time)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
)

select * from borrows
union all
select * from repays
union all
select * from liquidations
