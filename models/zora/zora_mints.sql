{{ config(
    schema = 'zora',
    alias = 'mints',
    post_hook='{{ expose_spells(\'["ethereum","optimism","base","zora"]\',
                    "project",
                    "zora",
                    \'["hildobby"]\') }}')
}}

{% set zora_mints_models = [
 ref('zora_ethereum_mints')
,ref('zora_optimism_mints')
,ref('zora_base_mints')
,ref('zora_zora_mints')
] %}

SELECT *
FROM (
    {% for zora_mints_model in zora_mints_models %}
    SELECT blockchain
    , block_time
    , block_number
    , token_standard
    , token_id
    , quantity
    , total_price
    , recipient
    , tx_hash
    , evt_index
    , contract_address
    FROM {{ zora_mints_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)