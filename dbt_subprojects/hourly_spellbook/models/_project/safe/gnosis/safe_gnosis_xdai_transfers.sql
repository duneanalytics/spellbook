{{ 
    config(
        schema = 'safe_gnosis',
        alias = 'xdai_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(blockchains = \'["gnosis"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke"]\') }}'
    )
}}

{% set project_start_date = '2020-05-21' %}

{{
    safe_native_transfers(
        blockchain = 'gnosis',
        native_token_symbol = 'XDAI',
        project_start_date = project_start_date
    )
}}

union all

select
    'gnosis' as blockchain,
    'XDAI' as symbol,
    s.address,
    try_cast(date_trunc('day', a.evt_block_time) as date) as block_date,
    CAST(date_trunc('month', a.evt_block_time) as DATE) as block_month,
    a.evt_block_time as block_time,
    CAST(a.amount AS INT256) as amount_raw,
    a.evt_tx_hash as tx_hash,
    cast(a.evt_index as varchar) as trace_address,
    p.price * CAST(a.amount AS DOUBLE) / 1e18 AS amount_usd
from {{ source('xdai_gnosis', 'BlockRewardAuRa_evt_AddedReceiver') }} a
join {{ ref('safe_gnosis_safes') }} s
    on a.receiver = s.address
left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = 'XDAI'
    and p.minute = date_trunc('minute', a.evt_block_time)
{% if not is_incremental() %}
where a.evt_block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where a.evt_block_time > date_trunc('day', now() - interval '10' day)
{% endif %}
