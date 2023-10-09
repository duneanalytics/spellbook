{{
    config(
        tags=['dunesql'],
        schema = 'balances_ethereum',
        alias = alias('erc20_supply'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "balances",
                                \'["Henrystats"]\') }}'
    )
}}

WITH 

minter_addresses as (
    SELECT 
        MIN_BY("from", evt_block_time) as mint_address, 
        contract_address as token_address 
    FROM 
    {{ source('erc20_ethereum', 'evt_Transfer') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    GROUP BY 2 
),

hourly_total_supply as (
    SELECT 
        SUM(COALESCE(t.amount, t.amount_raw/1e18)) as total_supply, 
        t.token_address
    FROM 
    {{ ref('balances_ethereum_erc20_latest') }} t 
    INNER JOIN 
    minter_addresses m 
        ON t.token_address = m.token_address
        AND t.wallet_address != m.mint_address
    GROUP BY 2
)

SELECT * FROM hourly_total_supply