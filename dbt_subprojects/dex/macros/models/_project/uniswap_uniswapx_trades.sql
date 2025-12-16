{% macro uniswap_uniswapx_trades(
    blockchain = null
    , uniswapx_contracts = []
    , start_date = '2023-07-17'
    , native_token_address = '0x0000000000000000000000000000000000000000'
    )
%}

with 

fill_events as (
    select 
        block_date,
        block_number,
        block_time,
        tx_from,
        tx_to,
        tx_hash,
        index as evt_index,
        tx_index,
        contract_address,
        topic1 as order_hash,
        bytearray_substring(topic2, 13, 20) as filler,
        bytearray_substring(topic3, 13, 20) as swapper
    from 
    {{ source(blockchain, 'logs') }}
    where block_date >= date '{{start_date}}'
    and contract_address in (
    {% for addr in uniswapx_contracts %}
        {{ addr }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    )
    {% if is_incremental() %}
    and topic0 = 0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66 -- fill event 
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

erc20_tfers as (
    select 
        erc.evt_block_date as block_date,
        erc.evt_block_time as block_time, 
        erc.evt_block_number as block_number,
        erc.evt_tx_hash as tx_hash,
        erc."from",
        erc.to,
        erc.value,
        erc.contract_address 
    from 
    {{ source('erc20_' + blockchain, 'evt_Transfer') }} erc 
    inner join 
    fill_events fve 
        on erc.evt_block_number = fve.block_number
        and erc.evt_block_date = fve.block_date
        and erc.evt_tx_hash = fve.tx_hash
    where erc.evt_block_date >= date '{{start_date}}'
    and erc.value > uint256 '0'
    {% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

blockchain_traces as (
    select 
        erc.block_date,
        erc.block_time,
        erc.block_number,
        erc.tx_hash,
        erc."from",
        erc.to,
        erc.value,
        {{ native_token_address }} as contract_address 
    from 
    {{ source(blockchain, 'traces') }} erc 
    inner join 
    fill_events fve 
        on erc.block_number = fve.block_number
        and erc.block_date = fve.block_date
        and erc.tx_hash = fve.tx_hash
    where erc.block_date >= date '{{start_date}}'
    and erc.value > uint256 '0'
    {% if is_incremental() %}
    and {{ incremental_predicate('erc.block_time') }}
    {% endif %} 
)

select 
    '{{blockchain}}' as blockchain,
    'uniswap' as project,
    'uniswapx' as version, 
    cast(date_trunc('month', fe.block_time) as date) as block_month,
    cast(date_trunc('day', fe.block_time) as date) as block_date,
    fe.block_time, 
    fe.block_number,
    coalesce(transfer_to.value, eth_out.value) as token_bought_amount_raw, 
    coalesce(transfer_from.value, eth_in.value) as token_sold_amount_raw, 
    coalesce(transfer_to.contract_address, eth_out.contract_address) as token_bought_address,
    coalesce(transfer_from.contract_address, eth_in.contract_address) as token_sold_address,
    fe.swapper as taker,
    fe.filler as maker, 
    fe.contract_address as project_contract_address,
    fe.tx_hash,
    fe.evt_index,
    fe.swapper as sender,
    fe.tx_from,
    fe.tx_to,
    fe.tx_index 
from 
fill_events fe 
left join 
erc20_tfers transfer_from 
    on fe.block_number = transfer_from.block_number 
    and fe.block_date = transfer_from.block_date 
    and fe.tx_hash = transfer_from.tx_hash 
    and fe.swapper = transfer_from."from"
left join 
erc20_tfers transfer_to
    on fe.block_number = transfer_to.block_number 
    and fe.block_date = transfer_to.block_date 
    and fe.tx_hash = transfer_to.tx_hash 
    and fe.swapper = transfer_to."to"
left join 
blockchain_traces eth_in
    on fe.block_number = eth_in.block_number 
    and fe.block_date = eth_in.block_date 
    and fe.tx_hash = eth_in.tx_hash 
    and fe.swapper = eth_in."from"
left join 
blockchain_traces eth_out
    on fe.block_number = eth_out.block_number 
    and fe.block_date = eth_out.block_date 
    and fe.tx_hash = eth_out.tx_hash 
    and fe.swapper = eth_out."to"
{% endmacro %}