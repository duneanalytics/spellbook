{{
    config(
        schema = 'blend_ethereum',
        alias='events',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        unique_key = ['tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "blend",
                                \'["hildobby"]\') }}'
    )
}}

{% set blend_event_models = [
     ('loan_started', ref('blend_ethereum_loan_taken'))
     , ('auctioned', ref('blend_ethereum_auctioned'))
     , ('buy_locked', ref('blend_ethereum_buy_locked'))
     , ('repaid', ref('blend_ethereum_repaid'))
     , ('refinance', ref('blend_ethereum_refinanced'))
     , ('seize', ref('blend_ethereum_seized'))
] %}

SELECT *
FROM (
        {% for blend_event_model in blend_event_models %}
        SELECT
        '{{ contracts_model[0] }}' AS event_type
        , *
        FROM {{ blend_event_model[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );