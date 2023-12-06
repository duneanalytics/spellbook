{{ config(
    schema = 'zora',
    alias = 'mints',
    materialized='incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    post_hook='{{ expose_spells(\'["ethereum","optimism","base","zora","goerli"]\',
                    "project",
                    "zora",
                    \'["hildobby", "jeff-dude"]\') }}')
}}

{% set zora_mints_models = [
 ref('zora_ethereum_mints')
,ref('zora_optimism_mints')
,ref('zora_base_mints')
,ref('zora_zora_mints')
,ref('zora_goerli_mints')
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
    , total_price_usd
    , recipient
    , tx_hash
    , evt_index
    , contract_address
    , tx_from
    , tx_to
    FROM {{ zora_mints_model }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)