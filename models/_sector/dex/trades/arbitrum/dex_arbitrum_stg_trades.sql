{{ config(
    schema = 'dex_arbitrum',
    alias = 'stg_trades',
    materialized = 'view'
    )
}}


-- (blockchain, project, project_version, model)
{% set base_models = [
    ref('uniswap_v3_arbitrum_stg_trades')
] %}

WITH base_union AS (
    SELECT * FROM (
        {% for base_model in base_models %}
        SELECT
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
            evt_index,
            row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
        FROM {{ base_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
    WHERE duplicates_rank = 1
)

{{ add_tx_from_and_to('base_union', 'arbitrum') }}