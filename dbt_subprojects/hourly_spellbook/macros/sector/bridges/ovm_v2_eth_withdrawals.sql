{% macro ovm_v2_withdrawals(blockchain, l1_portal) %}

-- L2 to L1 ETH Withdrawals for OVM
-- 
-- The general flow for an ETH Withdrawal in OVM is going through the L1StandardBridge.
-- However, in OVM, Message Passing is permissionless. In turn, the Portal can, and is many times called directly
-- The user hence can have three sources:
-- 1. Portal (direct)
-- 2. Messenger -> Portal (direct)
-- 3. L1StandardBridge -> Messenger -> Portal (calling the L1StandardBridge)
-- 4. Custom Bridge -> Messenger -> Portal (calling the L1StandardBridge)
-- 5. Custom Bridges through the Messenger, not yet supported

-- important addresses
-- {% set l1_portal = 0x49048044D57e1C92A77f79988d21Fa8fAF74E97e %} -- for base
{% set cross_domain_messenger = 0x4200000000000000000000000000000000000007 %}
{% set l2_standard_bridge = 0x4200000000000000000000000000000000000010 %}
{% set l2_to_l1_message_passer = 0x4200000000000000000000000000000000000016 %}

with withdrawal_l2_to_l1_portal_call as (
    select
        '{{blockchain}}' as deposit_chain
        , 'ethereum' as withdrawal_chain
        , 'Standard Bridge' as bridge_name
        , 2 as bridge_version
        , call_block_date as block_date
        , call_block_number as block_number
        , cast(JSON_EXTRACT_SCALAR(c._tx, '$.value') AS uint256) as withdrawal_amount_raw
        , from_hex(JSON_EXTRACT_SCALAR(c._tx, '$.sender')) as sender
        , from_hex(JSON_EXTRACT_SCALAR(c._tx, '$.target')) as recipient
        , 'eth' as withdrawal_token_standard
        , 0x0000000000000000000000000000000000000000 as withdrawal_token_address
        , call_tx_from as tx_from
        , call_tx_hash as tx_hash
        , {{l1_portal}} as contract_address --OptimismPortal
        , ROW_NUMBER() over (
            partition by
                c.call_tx_hash
            order by
                c.call_tx_index
        ) as matching_index
        , from_hex(JSON_EXTRACT_SCALAR(c._tx, '$.data')) as data
        from bridge{{blockchain}}_ethereum.optimismportal_call_finalizewithdrawaltransaction c
        where c.call_success = true
   and CAST(JSON_EXTRACT_SCALAR(c._tx, '$.value') as uint256) > 0
)
, withdrawal_l2_to_l1_portal_event as (
        select
        e.evt_tx_hash as tx_hash
        , e.evt_index
        , ROW_NUMBER() over (
            partition by
                e.evt_tx_hash
            order by
                e.evt_index
        ) as matching_index
        , withdrawalHash as bridge_transfer_id
        from bridge{{blockchain}}_ethereum.optimismportal_evt_withdrawalfinalized e
        where success = true
),
withdrawal_portal_raw as (
select
    c.*
    , e.evt_index
    , bridge_transfer_id
from withdrawal_l2_to_l1_portal_call c
inner join 
withdrawal_l2_to_l1_portal_event e on 
c.tx_hash = e.tx_hash
and c.matching_index = e.matching_index
),
direct_l2_to_l1_eth_withdrawals as (
--portal
select 
    deposit_chain
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , block_date
    , block_number
    , withdrawal_amount_raw
    , sender
    , recipient
    , withdrawal_token_standard
    , withdrawal_token_address
    , tx_from
    , tx_hash
    , contract_address
    , bridge_transfer_id
from withdrawal_portal_raw
where destination <> {{cross_domain_messenger}}
),

messenger_l2_to_l1_eth_withdrawals as (
select 
    deposit_chain
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , block_date
    , block_number
    , withdrawal_amount_raw
    , varbinary_substring(data, 49, 20) as sender
    , varbinary_substring(data, 81, 20) as recipient
    , withdrawal_token_standard
    , withdrawal_token_address
    , tx_from
    , tx_hash
    , recipient as contract_address
    , bridge_transfer_id
from withdrawal_portal_raw
where sender = {{cross_domain_messenger}}
and varbinary_substring(data, 49, 20) <> {{l2_standard_bridge}} -- L2StandardBridge
),

l1_standard_bridge_l2_to_l1_eth_withdrawals as (
select 
    deposit_chain
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , block_date
    , block_number
    , withdrawal_amount_raw
    , varbinary_substring(data, 235, 20) as sender -- taken from the finalizeBridgeETH() selector
    , varbinary_substring(data, 277, 20) as recipient -- taken from the finalizeBridgeETH() selector
    , withdrawal_token_standard
    , withdrawal_token_address
    , tx_from
    , tx_hash
    , varbinary_substring(data, 81, 20) as contract_address
    , bridge_transfer_id
from withdrawal_portal_raw
where sender = {{cross_domain_messenger}}
and varbinary_substring(data, 49, 20) = {{l2_standard_bridge}} -- L2StandardBridge
)

select * from direct_l2_to_l1_eth_withdrawals
union all
select * from messenger_l2_to_l1_eth_withdrawals
union all
select * from l1_standard_bridge_l2_to_l1_eth_withdrawals