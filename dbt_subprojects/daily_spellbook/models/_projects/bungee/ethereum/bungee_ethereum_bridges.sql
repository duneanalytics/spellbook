{{
    config(
        schema = 'bungee_ethereum',
        alias = 'bridges',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['transfer_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

with source_data as (
    {{ bungee_SocketBridge('ethereum') }}
),

tokens_mapped as (
    select
        *,
        case
            when token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
            then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            else token
        end as token_adjusted
    from source_data
),

price_data as (
    select 
        tokens_mapped.*,
        p.price * amount / power(10, p.decimals) as amount_usd
    from tokens_mapped
    left join {{ source('prices', 'usd') }} p 
        on p.contract_address = tokens_mapped.token_adjusted
        and p.blockchain = 'ethereum'
        and p.minute = date_trunc('minute', tokens_mapped.evt_block_time)
        {% if is_incremental() %}
        and {{ incremental_predicate('p.minute') }}
        {% endif %}
)

select * from price_data 