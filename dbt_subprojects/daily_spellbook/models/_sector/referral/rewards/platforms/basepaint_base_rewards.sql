{{ config(
    schema = 'basepaint_base',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

-- basepaint address 0xba5e05cb26b78eda3a2f8e3b3814726305dcac83
-- bprewards address 0xaff1a9e200000061fc3283455d8b0c7e3e728161
--
-- The referral contract mints the new basepaint nfts to itself and
-- transfers them to the minter, and mints an erc20 to the referrer
-- that can be used to withdraw the referral rewards.
--
-- We get all mints by taking the union of all BasePaintRewards ERC20
-- Transfer events from address(0) to an address (= mint with a refererrer)
-- and all Basepaint ERC1155 TransferSingle events from address(0) (= mint)
-- where the transfer is NOT to the rewards contract (= mint without a referrer)

select
    'base' as blockchain
    ,'basepaint' as project
    ,'v1' as version
    ,evt_block_number as block_number
    ,evt_block_time as block_time
    ,cast(date_trunc('day',evt_block_time) as date) as block_date
    ,cast(date_trunc('month',evt_block_time) as date) as block_month
    ,evt_tx_hash as tx_hash
    ,'NFT' as category
    ,e."to" as referrer_address
    ,tx."from" as referee_address
    ,{{ var("ETH_ERC20_ADDRESS") }} as currency_contract
    ,e."value" as reward_amount_raw
    ,0xBa5e05cb26b78eDa3A2f8e3b3814726305dcAc83 as project_contract_address
    ,evt_index as sub_tx_id
    ,tx."from" as tx_from
    ,tx."to" as tx_to
from {{source('basepaint_base', 'BasePaintRewards_evt_Transfer')}} e
inner join {{source('base', 'transactions')}} tx
    on evt_block_number = tx.block_number
    and evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
where
    e."from" = 0x0000000000000000000000000000000000000000
{% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

select
    'base' as blockchain
    ,'basepaint' as project
    ,'v1' as version
    ,evt_block_number as block_number
    ,evt_block_time as block_time
    ,cast(date_trunc('day',evt_block_time) as date) as block_date
    ,cast(date_trunc('month',evt_block_time) as date) as block_month
    ,evt_tx_hash as tx_hash
    ,'NFT' as category
    ,0x0000000000000000000000000000000000000000 as referrer_address
    ,e."to" as referee_address
    ,{{ var("ETH_ERC20_ADDRESS") }} as currency_contract
    ,0 as reward_amount_raw
    ,0xBa5e05cb26b78eDa3A2f8e3b3814726305dcAc83 as project_contract_address
    ,evt_index as sub_tx_id
    ,tx."from" as tx_from
    ,tx."to" as tx_to
from {{source('basepaint_base', 'BasePaint_evt_TransferSingle')}} e
inner join {{source('base', 'transactions')}} tx
    on evt_block_number = tx.block_number
    and evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
where
    e."from" = 0x0000000000000000000000000000000000000000
    and e."to" != 0xaff1a9e200000061fc3283455d8b0c7e3e728161
{% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
{% endif %}
