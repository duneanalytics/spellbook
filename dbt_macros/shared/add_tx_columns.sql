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
    on
    model.block_date = tx.block_date
    and model.block_number = tx.block_number
    and model.tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% endmacro %}

##########################################################################################
#### multichain beta #####################################################################
##########################################################################################

{% macro add_tx_columns_multichain(
    model_cte
    , blockchains
    , columns = []
    )
%}

{% for blockchain in blockchains %}
select
    model.*
    {% for column in columns %}
    , tx."{{column}}" as tx_{{column}}
    {% endfor %}
from (
    select * from {{model_cte}}
    where blockchain = '{{blockchain}}'
) model
inner join {{source(blockchain, 'transactions')}} tx
    on
    model.block_date = tx.block_date
    and model.block_number = tx.block_number
    and model.tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
{% endmacro %}