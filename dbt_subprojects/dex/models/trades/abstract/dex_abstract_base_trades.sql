{{ config(
    schema = 'dex_abstract'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('abstractswap_v3_abstract_base_trades')
    ,ref('abstractswap_v2_abstract_base_trades')
] %}

with
    base_union as (
        select *
        from
            (
                {% for base_model in base_models %}
                    select
                        blockchain,
                        project,
                        version,
                        block_month,
                        block_date,
                        block_time,
                        block_number,
                        token_bought_amount_raw,
                        token_sold_amount_raw,
                        token_bought_address,
                        token_sold_address,
                        taker,
                        maker,
                        project_contract_address,
                        tx_hash,
                        evt_index
                    from {{ base_model }}
                    {% if not loop.last %}
                        union all
                    {% endif %}
                {% endfor %}
            )
    )

    {{
    add_tx_columns(
        model_cte = 'base_union'
        , blockchain = 'abstract'
        , columns = ['from', 'to', 'index']
    )
}}
