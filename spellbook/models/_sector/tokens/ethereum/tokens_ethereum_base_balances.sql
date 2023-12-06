{{ config(
        schema = 'tokens_ethereum',
        alias = 'base_balances',
        partition_by = ['token_standard', 'block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['token_standard', 'block_number', 'tx_index', 'token_address', 'wallet_address'],
        )
}}

select * from {{ref('tokens_ethereum_base_balances_erc20')}}
{% if is_incremental() %}
where {{incremental_predicate('block_time')}}
{% endif %}

UNION ALL
select * from {{ref('tokens_ethereum_base_balances_native')}}
{% if is_incremental() %}
where {{incremental_predicate('block_time')}}
{% endif %}
