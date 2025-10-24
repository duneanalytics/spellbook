{% set blockchain = 'abstract' %}

{{ config(
        schema = 'uniswap_v2_forks_' + blockchain,
        alias = 'decoded_swap_events',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

with pool_events as (   
    select 
        '{{blockchain}}' as blockchain
        , * 
    from (
        {{uniswap_v2_pool_event_decoding(
            logs = source(blockchain, 'logs')
        )}}
    )
)
select
    *
from
    pool_events