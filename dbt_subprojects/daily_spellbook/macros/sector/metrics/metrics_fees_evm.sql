{% macro metrics_fees_evm(blockchain) %}

select
    blockchain
    , block_date
    , sum(tx_fee_usd) as gas_fees_usd
from
    {{ source('gas', 'fees') }}
where blockchain = '{{blockchain}}'
{% if is_incremental() or true %}
and
    {{ incremental_predicate('block_date') }}
{% endif %}
group by
    blockchain
    ,block_date

{% endmacro %}
