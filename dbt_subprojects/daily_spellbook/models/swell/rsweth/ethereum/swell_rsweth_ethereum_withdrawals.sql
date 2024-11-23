{{ config(
        schema = 'swell_rsweth_ethereum',
        alias = 'withdrawals',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_id'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "swell",
                                \'["maybeYonas"]\') }}'
        )
}}

{% set incremental = """
        where evt_block_time >= (
            select min(request_block_time) 
            from {{this}} 
            where claim_block_time is null
        )
"""%}

with 
rsweth_decoded_withdrawal_requests as (
    select 
        evt_block_time, evt_tx_hash, evt_block_number, evt_index,
        lastTokenIdProcessed,
        owner,
        tokenId,
        amount,
        timestamp,
        rateWhenCreated
    from {{source ('swell_v3_ethereum', 'RswEXIT_evt_WithdrawRequestCreated')}}
    -- from swell_v3_ethereum.RswEXIT_evt_WithdrawRequestCreated

    {% if is_incremental() %}
    {{incremental}}
    {% endif %}
),
rsweth_decoded_withdrawal_claimed as (
    select 
        evt_block_time, evt_tx_hash, evt_block_number, evt_index,
        owner,
        tokenId,
        exitClaimedETH
    from {{source ('swell_v3_ethereum', 'RswEXIT_evt_WithdrawalClaimed')}}
    -- from swell_v3_ethereum.RswEXIT_evt_WithdrawalClaimed
    {% if is_incremental() %}
    {{incremental}}
    {% endif %}

),
rsweth_decoded_withdrawal_processed as (
    select 
        evt_block_time, evt_tx_hash, evt_block_number, evt_index,
        fromTokenId,
        toTokenId,
        processedRate,
        processedExitingETH,
        processedExitedETH
    from {{source ('swell_v3_ethereum', 'RswEXIT_evt_WithdrawalsProcessed')}}
    -- from swell_v3_ethereum.RswEXIT_evt_WithdrawalsProcessed
    {% if is_incremental() %}
    {{incremental}}
    {% endif %}
)

select 
    r.evt_block_time as request_block_time,
    r.evt_tx_hash as request_tx_hash,
    r.evt_block_number as request_block_number,
    r.evt_index as request_index,
    r.tokenId as token_id,
    r.owner as owner,
    r.amount as rswETH_amount,
    r.rateWhenCreated as rswETH_request_rate,

    p.evt_block_time as processed_block_time,
    p.evt_tx_hash as processed_tx_hash,
    p.evt_block_number as processed_block_number,
    p.evt_index as processed_index,
    p.processedRate as rswETH_processed_rate,
    
    c.evt_block_time as claim_block_time,
    c.evt_tx_hash as claim_tx_hash,
    c.evt_block_number as claim_block_number,
    c.evt_index as claim_index,
    c.exitClaimedETH as ETH_amount
from rsweth_decoded_withdrawal_requests r
    left join rsweth_decoded_withdrawal_processed p 
        on r.tokenId >= p.fromTokenId
        and r.tokenId <= p.toTokenId
    left join rsweth_decoded_withdrawal_claimed c
        on r.tokenId = c.tokenId
order by 1 desc