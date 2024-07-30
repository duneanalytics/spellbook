{{ config(

        schema = 'rebase',
        alias ='events',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "rebase",
                                    contributors = \'["hildobby"]\') }}'
)
}}

{% set rebase_models = [
 ref('rebase_ethereum_events')
] %}

SELECT *
FROM (
    {% for rebase_model in rebase_models %}
    SELECT blockchain
    , token_address
    , token_symbol
    , block_time
    , block_number
    , rebase_rate
    , tx_hash
    , evt_index
    FROM {{ rebase_model }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
