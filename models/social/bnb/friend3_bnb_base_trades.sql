{{ config(
    schema = 'friend3_bnb',
    tags = ['dunesql'],
    alias = alias('base_trades')
    )
}}

{% set friend3_start_date = '2023-09-20' %}

SELECT 'bnb' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'friend3' AS project
, trader
, subject
, CASE WHEN isBuy = TRUE THEN 'buy' ELSE 'sell' END AS trade_side
, ethAmount/1e18 AS amount_original
, ticketAmount AS share_amount
, subjectEthAmount/1e18 AS subject_fee_amount
, protocolEthAmount/1e18 AS protocol_fee_amount
, 0x0000000000000000000000000000000000000000 AS currency_contract
, 'ETH' AS currency_symbol
, supply
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{source('friend3_v1_bnb', 'Friend3V1_evt_Trade')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% else %}
WHERE evt_block_time >= TIMESTAMP '{{friend3_start_date}}'
{% endif %}