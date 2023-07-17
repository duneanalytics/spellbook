{{ config(alias = alias('signer_thresholds'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "safe",
                                    \'["gentrexha"]\') }}'
)}}

-- PoC Query here - https://dune.com/queries/2116702
with safes as (
    select
        call_block_time as block_time,
        et.`from` as address,
        cardinality(_owners) as num_owners,
        _threshold as threshold
    from {{ source('gnosis_safe_ethereum', 'Safev0_1_0_call_setup') }} s
    join {{ source('ethereum', 'traces') }} et
        on s.call_tx_hash = et.tx_hash and s.call_trace_address = et.trace_address
    where
        s.call_success = true
        and et.success = true
        AND substring(cast(et.input as varchar(8)), 0, 4) in ('0x0ec78d9e') -- setup methods of v0_1_0
        AND et.call_type = 'delegatecall' -- the delegate call to the master copy is the Safe address
        AND cast(et.to as varchar(42)) in ('0x8942595A2dC5181Df0465AF0D7be08c8f23C93af') -- mastercopy address v0_1_0
    union all
    select
        call_block_time as block_time,
        contract_address as address,
        cardinality(_owners) as num_owners,
        _threshold as threshold
    from
        {{ source('gnosis_safe_ethereum', 'Safev1_0_0_call_setup') }}
    where
        call_success = true
    union all
    select
        call_block_time as block_time,
        contract_address as address,
        cardinality(_owners) as num_owners,
        _threshold as threshold
    from
        {{ source('gnosis_safe_ethereum', 'Safev1_1_0_call_setup') }}
    where
        call_success = true
    union all
    select
        call_block_time as block_time,
        contract_address as address,
        cardinality(_owners) as num_owners,
        _threshold as threshold
    from
        {{ source('gnosis_safe_ethereum', 'Safev1_1_1_call_setup') }}
    where
        call_success = true
    union all
    select
        evt_block_time as block_time,
        contract_address as address,
        cardinality(owners) as num_owners,
        threshold
    from
        {{ source('gnosis_safe_ethereum', 'GnosisSafev1_3_0_evt_SafeSetup') }}
),

threshold_changes as (
    select
        evt_block_time as block_time,
        threshold,
        contract_address as address
    from
        {{ source('gnosis_safe_ethereum', 'Safev0_1_0_evt_ChangedThreshold') }}
    union all
    select
        evt_block_time as block_time,
        threshold,
        contract_address as address
    from
        {{ source('gnosis_safe_ethereum', 'Safev1_0_0_evt_ChangedThreshold') }}
    union all
    select
        evt_block_time as block_time,
        threshold,
        contract_address as address
    from
        {{ source('gnosis_safe_ethereum', 'Safev1_1_0_evt_ChangedThreshold') }}
    union all
    select
        evt_block_time as block_time,
        threshold,
        contract_address as address
    from
        {{ source('gnosis_safe_ethereum', 'Safev1_1_1_evt_ChangedThreshold') }}
    union all
    select
        evt_block_time as block_time,
        threshold,
        contract_address as address
    from
        {{ source('gnosis_safe_ethereum', 'GnosisSafev1_3_0_evt_ChangedThreshold') }}
),

data as (
    select
        block_time,
        address,
        threshold
    from
        safes
    union all
    select
        block_time,
        address,
        threshold
    from
        threshold_changes
),

current_thresholds as (
    SELECT
        a.address,
        a.threshold
    FROM (
        SELECT
            address,
            threshold,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_time DESC) ranked_order
            FROM data
        ) a
     WHERE a.ranked_order = 1
)

select
    'ethereum' as blockchain,
    address,
    threshold
from
    current_thresholds
