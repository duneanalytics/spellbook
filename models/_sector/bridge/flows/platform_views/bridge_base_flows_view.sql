
{{ config(
        schema = 'base',
        alias = 'flows',
        
        materialized = 'view',
        unique_key = ['blockchain','tx_hash','evt_index'],
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "base",
                                    \'["hildobby"]\') }}')
}}

SELECT *
FROM {{ ref('bridge_flows') }}
WHERE project = 'base'
