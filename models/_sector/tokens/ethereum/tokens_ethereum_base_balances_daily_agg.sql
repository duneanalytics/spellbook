{{ config(
        schema = 'tokens_ethereum',
        alias = 'base_balances_daily_agg',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['unique_key'],
        )
}}
select
    *,
    {{ dbt_utils.generate_surrogate_key(['day', 'type', 'address', 'contract_address', 'token_id']) }} as unique_key
    from (
        select
            cast(date_trunc('day', block_time) as date) as day,
            block_number,
            block_time,
            "type",
            "address",
            contract_address,
            token_id,
            amount,
            row_number() OVER (partition by date_trunc('day', block_time), type, address, contract_address, token_id order by block_number desc) as row_number
        from {{ source('tokens_ethereum', 'balances_ethereum_0004') }} balances
        {% if is_incremental() %}
        WHERE {{incremental_predicate('block_time')}}
        {% endif %}
    ) where row_number = 1
