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
    -- Static list of RWA token mint addresses from rwa.xyz
    select token_mint_address from (
        values
            -- Stablecoins
            ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),  -- USDC (Circle)
            ('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'),  -- USDT (Tether)
            ('2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo'),  -- PYUSD (Paxos)
            ('2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH'),  -- USDG (Paxos)
            ('USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB'),  -- USD1 (BitGo)
            ('9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u'),  -- FDUSD (First Digital Labs)
            ('HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr'),  -- EURC (Circle)
            ('USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA'),  -- USDS (Sky)
            ('AUSD1jCcCyPLybk1YnvPWsHQSrZ46dxwoMniN4N2UEB9'),  -- AUSD (Agora Ledger)
            ('6FrrzDk5mQARGc1TDYoyVnSyRdds1t4PbtohCD6p3tgG'),  -- USX (Solstice)
            -- U.S. Treasuries
            ('GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr'),  -- BUIDL (Securitize/BlackRock)
            ('A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6'),  -- USDY (Ondo)
            ('i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc'),  -- OUSG (Ondo)
            ('34mJztT9am2jybSukvjNqRjgJBZqHJsHnivArx1P4xy1'),  -- VBILL (Securitize/VanEck)
            -- Institutional Funds
            ('5Y8NV33Vv7WbnLfq3zBcKSdYPrk7g2KoiQoe7M2tcxp5'),  -- ONyc (OnRe)
            ('FubtUcvhSCr3VPXEcxouoQjKQ7NWTCzXyECe76B7L3f8'),  -- ACRED (Securitize/Apollo)
            -- Stocks (xStocks by Backed Finance)
            ('XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB'),  -- TSLAx (Tesla)
            ('Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh'),  -- NVDAx (NVIDIA)
            ('XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W'),  -- SPYx (S&P 500)
            ('Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ'),  -- QQQx (Nasdaq)
            ('XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ'),  -- MSTRx (MicroStrategy)
            ('XsueG8BtpquVJX9LVLLEGuViXUungE6WmK5YZ3p3bd1'),  -- CRCLx (CircleCI)
            ('XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN'),  -- GOOGLx (Alphabet)
            ('XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp'),  -- AAPLx (Apple)
            ('Xs7ZdzSHLU9ftNJsii5fCeJhoRWSC32SQGzGQtePxNu')   -- COINx (Coinbase)
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
