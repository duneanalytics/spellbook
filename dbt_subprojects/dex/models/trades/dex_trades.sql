{% set chains = [
    'gnosis'
] %}

{{ config(
    schema = 'dex'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                    "sector",
                                    "dex",
                                    \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi", "krishhh"]\') }}')
}}

-- keep existing dbt lineages for the following projects, as the team built themselves and use the spells throughout the entire lineage.
{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')
] %}

WITH as_is_dexs AS (
    {% for model in as_is_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM
        {{ model }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

, chain_dex_trades AS (
    SELECT *
    FROM (
        {% for chain in chains %}
            SELECT *
            FROM
                {{ ref('dex_'~chain~'_trades') }}
            {% if not loop.last %}
            UNION ALL
            {% endif %}
        {% endfor %}
    )
)

{% set cte_to_union = [
    'as_is_dexs'
    , 'chain_dex_trades'
    ]
%}

{% for cte in cte_to_union %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM
        {{ cte }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
