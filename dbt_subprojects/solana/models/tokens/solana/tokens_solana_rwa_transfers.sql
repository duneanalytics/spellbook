{{ config(
    schema = 'tokens_solana',
    alias = 'rwa_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "tokens",
                                \'["ilemi"]\') }}'
    )
}}

with rwa_tokens as (
    -- Static list of RWA token mint addresses
    -- Add your RWA token mint addresses here
    select token_mint_address from (
        values
            ('A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6'),  -- USDY (Ondo)
            ('9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u'),  -- FDUSD placeholder, add actual RWA tokens
            ('7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT')   -- UXD placeholder, add actual RWA tokens
    ) as t(token_mint_address)
),

token_start_dates as (
    -- Get the earliest start date for each RWA token from fungible dataset
    select
        f.token_mint_address,
        f.symbol,
        f.decimals,
        cast(f.created_at as date) as token_start_date
    from {{ source('tokens_solana', 'fungible') }} f
    inner join rwa_tokens rt
        on f.token_mint_address = rt.token_mint_address
),

filtered_transfers as (
    -- Filter transfers using partition columns and token start dates
    select
        t.block_time,
        t.block_date,
        t.block_slot,
        t.action,
        t.token_mint_address,
        t.amount,
        t.fee,
        t.token_version,
        t.from_owner,
        t.to_owner,
        t.from_token_account,
        t.to_token_account,
        t.tx_signer,
        t.tx_id,
        t.tx_index,
        t.outer_instruction_index,
        t.inner_instruction_index,
        t.outer_executing_account,
        tsd.symbol,
        tsd.decimals,
        cast(t.amount as double) / pow(10, tsd.decimals) as amount_decimal
    from {{ source('tokens_solana', 'transfers') }} t
    inner join token_start_dates tsd
        on t.token_mint_address = tsd.token_mint_address
    where 1=1
        -- Optimize by filtering on partition column with token start date
        and t.block_date >= tsd.token_start_date
        {% if is_incremental() %}
        and {{ incremental_predicate('t.block_date') }}
        {% endif %}
)

select * from filtered_transfers
