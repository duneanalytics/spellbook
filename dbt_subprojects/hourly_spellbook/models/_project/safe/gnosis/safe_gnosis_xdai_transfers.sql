{{ 
    safe_native_transfer_wrapper(
        blockchain = 'gnosis',
        date_filter = true
    )
}}

union all

-- Gnosis-specific: include block rewards
select
    'gnosis' as blockchain,
    'xDAI' as symbol,
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
    and p.symbol = 'xDAI'
    and p.minute = date_trunc('minute', a.evt_block_time)
{% if not is_incremental() %}
where a.evt_block_time > TIMESTAMP '2020-05-15' -- Gnosis Safe start date
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where a.evt_block_time > date_trunc('day', now() - interval '10' day)
{% endif %}
