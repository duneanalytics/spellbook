{% macro add_tx_columns(
    model_cte
    , blockchain
    , columns = []
    )
%}

select
    model.*
    {% for column in columns %}
    , tx."{{column}}" as tx_{{column}}
    {% endfor %}
from {{model_cte}} model
inner join {{source(blockchain, 'transactions')}} tx
    on model.block_number = tx.block_number
    and model.tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% endmacro %}