{% macro add_nft_tx_data(
    model_cte
    ,blockchain
    )
%}

select
    model.*
    , tx."from" as tx_from
    , tx."to" as tx_to
    , bytearray_reverse(bytearray_substring(bytearray_reverse(tx.data),1,32))  as tx_data_marker
from {{model_cte}} model
inner join {{source(blockchain, 'transactions')}} tx
    on model.block_number = tx.block_number
    and model.tx_hash = tx.hash
    {% if is_incremental() %}
    where {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% endmacro %}
