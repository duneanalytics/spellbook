{{ config(
        
        alias = 'addresses',
        schema = 'dex',
        unique_key = ['blockchain', 'address', 'dex_name', 'distinct_name'],
        post_hook='{{ expose_spells(\'["arbitrum", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                                "sector",
                                "dex",
                                \'["rantum"]\') }}'
        )
}}


{% set address_models = [  
    ref('dex_arbitrum_addresses')
    ,ref('dex_base_addresses')
    ,ref('dex_bnb_addresses')
    ,ref('dex_ethereum_addresses')
    ,ref('dex_optimism_addresses')
    ,ref('dex_polygon_addresses')

] %}


SELECT *
FROM (
    {% for model in address_models %}
    SELECT
    *
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

