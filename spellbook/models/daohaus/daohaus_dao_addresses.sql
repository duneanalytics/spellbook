{{ config(
    
    alias = 'dao_addresses',
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis"]\',
                                "project",
                                "daohaus",
                                \'["Henrystats"]\') }}')
}}

{% set daohaus_models = [
ref('daohaus_ethereum_dao_addresses')
,ref('daohaus_gnosis_dao_addresses')
] %}


SELECT *

FROM (
    {% for dao_model in daohaus_models %}
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