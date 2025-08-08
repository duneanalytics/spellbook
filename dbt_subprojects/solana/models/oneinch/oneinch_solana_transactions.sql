{{
    config(
        schema = 'oneinch_solana',
        alias = 'transactions',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['id', 'block_month']
    )
}}

-- pre-materialized light table for further use

with tx_list as ( -- for filtering using fast inner join
    select 
        call_tx_id as id
        , call_block_slot as block_slot
        , call_block_date as block_date 
    from {{ source('oneinch_solana', 'fusion_swap_call_fill') }}
    where 
        {% if is_incremental() %}
            {{ incremental_predicate('call_block_time') }}
        {% else %}
            call_block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
)



select
    'solana' as blockchain
    , id
    , block_slot
    , block_time
    , block_date
    , signer
    , signers
    , fee
    , cast(date_trunc('month', block_time) as date) as block_month
from {{ source('solana', 'transactions') }}
join tx_list using (id, block_slot, block_date)
where 
    {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
    {% else %}
        block_date >= date('{{ oneinch_cfg_macro("project_start_date") }}')
    {% endif %}

