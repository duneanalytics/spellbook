{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_daily',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['unique_key'],
        )
}}

select *,
 {{ dbt_utils.generate_surrogate_key(['day', 'type', 'address', 'contract_address', 'token_id']) }} as unique_key
 FROM (
{{
    balances_enrich(
        balances_base = ref('tokens_ethereum_base_balances_daily'),
        blockchain = 'ethereum',
        daily=true,
    )
}}
)
