{% macro metrics_transactions_evm(blockchain) %}

select
        blockchain
        , block_date
        , approx_distinct(tx_hash) as tx_count --max 2% error, which is fine
from
    {{ source('tokens', 'transfers') }}
where
    blockchain = '{{ blockchain }}'
    and amount_usd >=1
    {% if is_incremental() or true %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
group by
    blockchain
    , block_date

{% endmacro %}
