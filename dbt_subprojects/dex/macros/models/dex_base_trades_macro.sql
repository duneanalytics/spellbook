{% macro dex_base_trades_macro(
    blockchain,
    base_models,
    dedup = true,
    dedup_order_by = 'tx_hash',
    extra_filters = []
) %}

{%- if blockchain is none or blockchain == '' -%}
    {{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}

{%- if base_models | length == 0 -%}
    {{ exceptions.raise_compiler_error("base_models list cannot be empty") }}
{%- endif -%}

with base_union as (
        {% for base_model in base_models %}
        SELECT
            blockchain
            , project
            , version
            , block_month
            , block_date
            , block_time
            , block_number
            , cast(token_bought_amount_raw as uint256) as token_bought_amount_raw
            , cast(token_sold_amount_raw as uint256) as token_sold_amount_raw
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , evt_index
        FROM
            {{ base_model }}
        WHERE
           token_sold_amount_raw >= 0 and token_bought_amount_raw >= 0
        {% if var('dev_dates', false) -%}
            AND block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
        {%- else -%}
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
        {%- endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)

, add_tx_columns as (
    {{
        add_tx_columns(
            model_cte = 'base_union'
            , blockchain = blockchain
            , columns = ['from', 'to', 'index']
        )
    }}
)
{% if dedup %}
, final as (
    select
        *
        , row_number() over (partition by tx_hash, evt_index order by {{ dedup_order_by }}) as duplicates_rank
    from
        add_tx_columns
)
select
    *
from
    final
where
    duplicates_rank = 1
    {% for filter in extra_filters %}
    AND {{ filter }}
    {% endfor %}
{% else %}
select
    *
from
    add_tx_columns
    {% if extra_filters | length > 0 %}
where
    {% for filter in extra_filters %}
    {{ filter }}{% if not loop.last %} AND {% endif %}
    {% endfor %}
    {% endif %}
{% endif %}

{% endmacro %}
