{% macro ovm_v2_deposits(blockchain, l1_standard_bridge) %}

-- L2 to L1 ETH Deposits for OVM
--
-- The general flow for an ETH Deposit in OVM is to go through its bridge, known as the L2StandardBridge.
-- This contract then formats the message, sending it to the CrossDomainMessenger and then the L2ToL1MessagePasser.
-- However, in OVM, the Message Passing system is permissionless. In turn, the L2MessagePasser can, and is many times bypassed
-- The user hence can have three sources:
-- 1. L2ToL1MessagePasser (direct)
-- 2. CrossDomainMessenger -> L2ToL1MessagePasser (direct)
-- 3. L2StandardBridge -> CrossDomainMessenger -> L2ToL1MessagePasser (calling the L1StandardBridge)
-- 4. Custom Bridge -> CrossDomainMessenger -> L2ToL1MessagePasser (calling the L1StandardBridge)
-- 5. Custom Bridges through the L2ToL1MessagePasser, not yet supported
--
-- important addresses
{% set cross_domain_messenger = 0x4200000000000000000000000000000000000007 %}
{% set l2_standard_bridge = 0x4200000000000000000000000000000000000010 %}
{% set l2_to_l1_message_passer = 0x4200000000000000000000000000000000000016 %}
-- {% set l1_standard_bridge = 0x3154Cf16ccdb4C6d922629664174b904d80F2C35 %}

-- important methods
{% set relay_message_selector = 0xd764ad0b %}
{% set finalize_bridge_eth_selector = 0x1635f5fd %}


with direct_l2_to_l1_eth_deposits as (
-- 1. L2ToL1MessagePasser (direct)
select
    '{{blockchain}}' as deposit_chain
    , 'ethereum' as withdrawal_chain
    , 'Standard Bridge' as bridge_name
    , 2 as bridge_version
    , evt_block_date as block_date
    , evt_block_time as block_time
    , evt_block_number as block_number
    , value as deposit_amount_raw
    , sender
    , target as recipient 
    , 'eth' as deposit_token_standard
    , 0x0000000000000000000000000000000000000000 as deposit_token_address -- address(0)
    , evt_tx_from as tx_from
    , evt_tx_hash as tx_hash
    , evt_index
    , contract_address -- L2ToL1MessagePasser
    , withdrawalHash as bridge_id
from ovm_{{blockchain}}.l2tol1messagepasser_evt_messagepassed
where value <> 0
and sender <> {{cross_domain_messenger_address}}
),
cross_domain_messenger_l2_to_l1_eth_deposits as (
-- 2. CrossDomainMessenger -> L2ToL1MessagePasser (direct)
select
    '{{blockchain}}' as deposit_chain
    , 'ethereum' as withdrawal_chain
    , 'Standard Bridge' as bridge_name
    , 2 as bridge_version
    , evt_block_date as block_date
    , evt_block_time as block_time
    , evt_block_number as block_number
    , value as deposit_amount_raw -- taken from the relayMessage() CrossDomainMessenger
    , varbinary_substring(data, 49, 20) as sender -- taken from the relayMessage() CrossDomainMessenger
    , varbinary_substring(data, 81, 20) as recipient -- taken from the relayMessage() CrossDomainMessenger
    , 'eth' as deposit_token_standard
    , 0x0000000000000000000000000000000000000000 as deposit_token_address -- address(0)
    , evt_tx_from as tx_from
    , evt_tx_hash as tx_hash
    , evt_index
    , sender as contract_address
    , withdrawalHash as bridge_id
from ovm_{{blockchain}}.l2tol1messagepasser_evt_messagepassed
where value <> 0
and sender = {{cross_domain_messenger}}
and varbinary_substring(data, 81, 20) <> {{l1_standard_bridge}}
),
l2_standard_bridge_l2_to_l1_eth_deposits as (
-- 3. CrossDomainMessenger -> L2ToL1MessagePasser (calling the L1StandardBridge)
-- 4. Custom Bridge -> CrossDomainMessenger -> L2ToL1MessagePasser (calling the L1StandardBridge)
select
    '{{blockchain}}' as deposit_chain
    , 'ethereum' as withdrawal_chain
    , 'Standard Bridge' as bridge_name
    , 2 as bridge_version
    , evt_block_date as block_date
    , evt_block_time as block_time
    , evt_block_number as block_number
    , value as deposit_amount_raw
    , varbinary_substring(data, 235, 20) as sender -- taken from the finalizeBridgeETH() selector
    , varbinary_substring(data, 277, 20) as recipient -- taken from the finalizeBridgeETH() selector
    , 'eth' as deposit_token_standard
    , 0x0000000000000000000000000000000000000000 as deposit_token_address -- address(0)
    , evt_tx_from as tx_from
    , evt_tx_hash as tx_hash
    , evt_index
    , varbinary_substring(data, 49, 20) as sender -- taken from the relayMessage() CrossDomainMessenger
    , withdrawalHash as bridge_id
from ovm_{{blockchain}}.l2tol1messagepasser_evt_messagepassed
where value <> 0
and sender = {{cross_domain_messenger}}
and varbinary_substring(data, 81, 20) = {{l1_standard_bridge}}
)

select * from direct_l2_to_l1_eth_deposits
union all
select * from cross_domain_messenger_l2_to_l1_eth_deposits
union all
select * from l2_standard_bridge_l2_to_l1_eth_deposits