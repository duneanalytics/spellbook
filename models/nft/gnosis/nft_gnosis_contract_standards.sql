{{ config(
        alias ='contract_standards',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['nft_contract_address']
)
}}

 SELECT 'gnosis' as blockchain
, t.contract_address as nft_contract_address
, max_by(t.standard,t.block_time) AS standard
FROM {{ ref('nft_gnosis_transfers') }} t
    {% if is_incremental() %}
       WHERE t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
GROUP BY 1,2
