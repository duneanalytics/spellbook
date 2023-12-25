{{ config(
    
    alias = 'addresses',
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon", "base", "arbitrum"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}

{% set addresses_models = [
ref('dao_addresses_ethereum')
,ref('dao_addresses_gnosis')
,ref('dao_addresses_polygon')
,ref('dao_addresses_base')
,ref('dao_addresses_arbitrum')
] %}


SELECT *

FROM (
    {% for dao_model in addresses_models %}
    SELECT
        blockchain,
        dao_creator_tool, 
        dao, 
        dao_wallet_address,
        created_block_time,
        created_date
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)