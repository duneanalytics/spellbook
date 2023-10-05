{{ config(
    tags=['dunesql'],
    alias = alias('dao_addresses'),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "polygon"]\',
                                "project",
                                "syndicate",
                                \'["Henrystats"]\') }}')
}}

{% set syndicate_models = [
ref('syndicate_ethereum_dao_addresses')
,ref('syndicate_polygon_dao_addresses')
] %}


SELECT *

FROM (
    {% for dao_model in syndicate_models %}
    SELECT
        blockchain,
        dao_creator_tool, 
        dao, 
        dao_wallet_address,
        created_block_time,
        created_date,
        block_month
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)