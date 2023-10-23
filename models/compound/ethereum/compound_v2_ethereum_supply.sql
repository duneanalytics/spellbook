{{ config(
    
    schema = 'compound_v2_ethereum',
    alias = 'supply',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'evt_block_number', 'evt_index'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "compound_v2",
                                    \'["bizzyvinci", "hosuke"]\') }}'
    )
}}

with mints as (
    select
        '2' as version,
        'deposit' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        minter as depositor,
        cast(null as varbinary) as withdrawn_to,
        cast(null as varbinary) as liquidator,
        mintAmount / decimals_mantissa as amount,
        mintAmount / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        date_trunc('DAY', evt_block_time) as block_date,
        CAST(date_trunc('month', evt_block_time) as date) as block_month
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Mint') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc('day', now() - interval '7' day)
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Mint') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc('day', now() - interval '7' day)
		{% endif %}
    ) evt_mint
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_mint.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = date_trunc('minute', evt_mint.evt_block_time)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),
redeems as (
    select
        '2' as version,
        'withdraw' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        redeemer as depositor,
        redeemer as withdrawn_to,
        cast(null as varbinary) as liquidator,
        - (redeemAmount / decimals_mantissa) as amount,
        - (redeemAmount / decimals_mantissa) * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        date_trunc('DAY', evt_block_time) as block_date,
        CAST(date_trunc('month', evt_block_time) as date) as block_month
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Redeem') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc('day', now() - interval '7' day)
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Redeem') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc('day', now() - interval '7' day)
		{% endif %}
    ) evt_mint
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_mint.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = date_trunc('minute', evt_mint.evt_block_time)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)


select * from mints
union all
select * from redeems
