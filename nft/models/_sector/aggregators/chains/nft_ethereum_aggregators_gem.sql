{{config(
    schema = 'nft_ethereum',
    alias = 'aggregators_gem',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key='contract_address'
    )
}}
WITH vasa_contracts as (
    SELECT distinct
    address AS contract_address
    FROM {{ source('ethereum','creation_traces') }}
    WHERE "from" = 0x073ab1c0cad3677cde9bdb0cdeedc2085c029579
    and block_time >= TIMESTAMP '2021-10-12'
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
)


select
    c.contract_address
    ,'Gem' as name
from vasa_contracts c
left join {{ source('ethereum','transactions') }} t
on t.block_time >= CAST('2021-10-12' AS TIMESTAMP) and t.to = c.contract_address
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}
left join {{ ref('nft_ethereum_transfers') }} nt
on t.block_number = nt.block_number and t.hash = nt.tx_hash
{% if is_incremental() %}
AND {{incremental_predicate('nt.block_time')}}
{% endif %}
group by 1,2
having count(distinct t.hash) filter(where t."from" != 0x073ab1c0cad3677cde9bdb0cdeedc2085c029579) > 10
    and count(distinct nt.contract_address) > 2
