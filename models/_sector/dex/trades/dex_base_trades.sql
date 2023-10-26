{{ config(
    schema = 'dex',
    alias ='base_trades',
    partition_by = ['block_month', 'blockchain', 'project'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

{% set models = [
    ref('dex_arbitrum_base_trades')
    , ref('dex_base_base_trades')
    , ref('dex_bnb_base_trades')
    , ref('dex_celo_base_trades')
    , ref('dex_ethereum_base_trades')
    , ref('dex_optimism_base_trades')
    , ref('dex_polygon_base_trades')
] %}


with base_union as (
    SELECT *
    FROM
    (
        {% for model in models %}
        SELECT
            blockchain,
            project,
            version,
            block_month,
            block_date,
            block_time,
            block_number,
            token_bought_symbol,
            token_sold_symbol,
            token_pair,
            token_bought_amount,
            token_sold_amount,
            token_bought_amount_raw,
            token_sold_amount_raw,
            amount_usd,
            token_bought_address,
            token_sold_address,
            taker,
            maker,
            project_contract_address,
            tx_hash,
            tx_from,
            tx_to,
            evt_index
        FROM {{ model }}
        {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)
select *
from base_union