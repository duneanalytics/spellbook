{{ config(
    
    schema = 'dydx_ethereum',
    alias = 'votes',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "dydx",
                                \'["ivigamberdiev"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'dydx' %}
{% set dao_name = 'DAO: dYdX' %}
{% set dao_address = '0x7e9b1672616ff6d6629ef2879419aae79a9018d2' %}

WITH cte_sum_votes as
(SELECT sum(votingPower/1e18) as sum_votes,
        id
FROM {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_VoteEmitted') }}
GROUP BY id)

SELECT
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    cast(NULL as varchar) as version,
    vc.evt_block_time as block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash as tx_hash,
    '{{dao_name}}' as dao_name,
    {{dao_address}} as dao_address,
    vc.id as proposal_id,
    vc.votingPower/1e18 as votes,
    (votingPower/1e18) * (100) / (csv.sum_votes) as votes_share,
    p.symbol as token_symbol,
    p.contract_address as token_address,
    vc.votingPower/1e18 * p.price as votes_value_usd,
    vc.voter as voter_address,
    CASE WHEN vc.support = false THEN 'against'
         WHEN vc.support = true THEN 'for'
         END AS support,
    cast(NULL as varchar) as reason
FROM {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_VoteEmitted') }} vc
LEFT JOIN cte_sum_votes csv ON vc.id = csv.id
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
    AND p.symbol = 'DYDX'
    AND p.blockchain ='ethereum'