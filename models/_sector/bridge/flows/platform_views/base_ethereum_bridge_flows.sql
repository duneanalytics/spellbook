
{% set blockchain = 'ethereum' %}

{{ config(
        schema = 'base_' + blockchain,
        alias = 'bridge_flows',
        materialized = 'view',
        post_hook='{{ expose_spells(blockchains = \'[\"{{blockchain}}\"]\',
                                    spell_type = "sector",
                                    spell_name = "bridge",
                                    contributors = \'["hildobby"]\') }}')
}}

SELECT *
FROM {{ ref('bridge_flows_beta') }}
WHERE project = blockchain
