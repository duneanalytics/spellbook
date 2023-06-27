{{ config(
        tags=['prod_exclude'],
        alias ='sandwiches',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'sandwiched_pool', 'frontrun_tx_hash', 'frontrun_taker', 'frontrun_index', 'currency_address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set sandwiches_models = [
     (ref('dex_arbitrum_sandwiches'))
     , (ref('dex_avalanche_c_sandwiches'))
     , (ref('dex_bnb_sandwiches'))
     , (ref('dex_ethereum_sandwiches'))
     , (ref('dex_fantom_sandwiches'))
     , (ref('dex_gnosis_sandwiches'))
     , (ref('dex_optimism_sandwiches'))
     , (ref('dex_polygon_sandwiches'))
] %}

SELECT *
FROM (
        {% for sandwiches_model in sandwiches_models %}
        SELECT
        *
        FROM {{ sandwiches_model }}
        {% if not loop.last %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );