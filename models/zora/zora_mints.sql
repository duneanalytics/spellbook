{{ config(
    schema = 'zora',
    alias = 'mints',
    materialized='incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
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
    , tx_from
    , tx_to
    FROM {{ zora_mints_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)