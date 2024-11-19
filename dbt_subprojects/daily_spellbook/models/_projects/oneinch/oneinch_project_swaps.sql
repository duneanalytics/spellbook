{{  
    config(
        schema = 'oneinch',
        alias = 'project_swaps',
        materialized = 'view',
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address', 'call_trade_id']
    )
}}



select
    *
    , row_number() over(partition by user, project order by block_time) as user_project_sn
    , row_number() over(partition by user order by block_time, project) as user_over_sn
from ({% for blockchain in oneinch_project_swaps_exposed_blockchains_list() %}
    {{ "-- depends_on: {{ ref('oneinch_' + blockchain + '_project_orders') }}" }}
    
    select * from {{ ref('oneinch_' + blockchain + '_project_swaps') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %})