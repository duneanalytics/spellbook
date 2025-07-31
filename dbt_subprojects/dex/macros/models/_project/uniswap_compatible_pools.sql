{% macro uniswap_compatible_pools(
    blockchain = null
    , project = null
    , version = null
    , hardcoded_fee = null 
    , fee_column_name = null
    , pool_created_event = null
    , pool_column_name = 'pair'
    , token0_column_name = 'token0'
    , token1_column_name = 'token1'
    , hooks_column_name = null 
    )
%}

select 
    '{{ blockchain }}' as blockchain
    , '{{ project }}' as project
    , '{{ version }}' as version
    , contract_address 
    , evt_block_time as creation_block_time
    , evt_block_number as creation_block_number
    , f.{{ pool_column_name }} as id 
    {% if hardcoded_fee %} -- use hardcoded fee if it's exists
    , {{ hardcoded_fee }} as fee
    {% endif %}
    {% if fee_column_name %}
    , f.{{fee_column_name}} as fee -- use fee column if hardcoded fee doesn't exists
    {% endif %}
    {% if not (fee_column_name or hardcoded_fee) %}
    , cast(null as bigint) as fee 
    {% endif %}
    , evt_tx_hash as tx_hash
    , evt_index
    , f.{{ token0_column_name }} as token0
    , f.{{ token1_column_name }} as token1 
    {% if hooks_column_name %}
    , f.{{hooks_column_name}} as hooks 
    {% endif %}
from 
{{ pool_created_event }} f
{% if is_incremental() %}
where {{ incremental_predicate('f.evt_block_time') }}
{% endif %}

{% endmacro %}