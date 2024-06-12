
{% set blockchain = 'base' %}

{{ config(
        schema = blockchain,
        alias = 'flows',
        materialized = 'view',
        unique_key = ['blockchain','tx_hash','evt_index'],
        post_hook='{{ expose_spells(blockchains = \'[\"{{blockchain}}\"]\',
                                    spell_type = "sector",
                                    spell_name = "bridge",
                                    contributors = \'["hildobby"]\') }}')
}}

SELECT *
FROM {{ ref('bridge_flows_beta') }}
WHERE project = blockchain
