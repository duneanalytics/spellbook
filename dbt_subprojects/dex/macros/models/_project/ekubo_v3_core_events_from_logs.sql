{#
    Decodes Ekubo v3 Core events straight from raw logs, returning a subquery shaped like the
    corresponding Dune decoded table (ekubo_v3_<chain>.core_evt_*). Use it in place of source()
    on chains where Core has not been decoded yet; the output plugs into ekubo_compatible_pools
    and ekubo_compatible_liquidity_events unchanged.

    None of these events has indexed parameters, so every field is a 32-byte ABI word in data:
      PoolInitialized(bytes32 poolId, (address token0, address token1, bytes32 config) poolKey, int32 tick, uint96 sqrtRatio)
      PositionUpdated(address locker, bytes32 poolId, bytes32 positionId, int128 liquidityDelta, bytes32 balanceUpdate, bytes32 stateAfter)
      PositionFeesCollected(address locker, bytes32 poolId, bytes32 positionId, uint128 amount0, uint128 amount1)
      FeesAccumulated(bytes32 poolId, uint128 amount0, uint128 amount1)
#}
{% macro ekubo_v3_core_events_from_logs(
    blockchain = null
    , ekubo_core_contract = null
    , start_block_number = null
    , event = null
    )
%}

{%- set topic0 = {
    'PoolInitialized': '0x5e4688b340694b7c7fd30047fd082117dc46e32acfbf81a44bb1fac0ae65154d',
    'PositionUpdated': '0x704b3ab4a76158ad4d66625a2a43be81edbffd24630e8fde5174e97035370a07',
    'PositionFeesCollected': '0xd76ec32fbc3f07c70828b4f94343ee73279d0e8d4d2f28b018a4e67f37497753',
    'FeesAccumulated': '0xf7e050d866774820d81a86ca676f3afe7bc72603ee893f82e99c08fbde39af6c',
}[event] -%}

(
    select
        contract_address,
        block_time as evt_block_time,
        block_number as evt_block_number,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        index as evt_index,
    {%- if event == 'PoolInitialized' %}
        substr(data, 1, 32) as poolId,
        -- decoded tables expose tuples as JSON; match that so ekubo_compatible_pools can read it
        '{"token0":"0x' || lower(to_hex(substr(data, 45, 20)))
            || '","token1":"0x' || lower(to_hex(substr(data, 77, 20)))
            || '","config":"0x' || lower(to_hex(substr(data, 97, 32))) || '"}' as poolKey
    {%- elif event == 'PositionUpdated' %}
        substr(data, 33, 32) as poolId,
        substr(data, 129, 32) as balanceUpdate
    {%- elif event == 'PositionFeesCollected' %}
        substr(data, 33, 32) as poolId,
        varbinary_to_uint256(substr(data, 97, 32)) as amount0,
        varbinary_to_uint256(substr(data, 129, 32)) as amount1
    {%- elif event == 'FeesAccumulated' %}
        substr(data, 1, 32) as poolId,
        varbinary_to_uint256(substr(data, 33, 32)) as amount0,
        varbinary_to_uint256(substr(data, 65, 32)) as amount1
    {%- endif %}
    from {{ source(blockchain, 'logs') }}
    where block_number >= {{ start_block_number }}
    and contract_address = {{ ekubo_core_contract }}
    and topic0 = {{ topic0 }}
)

{% endmacro %}
