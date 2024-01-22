 {{
  config(
        
        schema = 'ethereum',
        alias = 'deployers', --ethereum.deployers
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ethereum",
                                    \'["ilemi"]\') }}')
}}

select 
c.name as contract_name
,c.namespace as contract_namespace
,c.address as contract_address
,cre."from" as contract_deployer
,ens.name as contract_deployer_ens
FROM {{ source('ethereum', 'contracts') }} c
LEFT JOIN {{ source('ethereum', 'creation_traces') }} cre ON cre.address = c.address
LEFT JOIN {{ ref('labels_ens') }} ens ON ens.address = cre."from"
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('c.created_at')}}
{% else %}
AND c.created_at >= now() - interval '1' day
{% endif %}