{% macro pyth_entropy_request(blockchain, symbol, entropy_address) %}
SELECT
    '{{ blockchain }}' as blockchain,
    tx_hash,
    from_big_endian_64(substr(output, 25, 8)) as assigned_sequence_number,
    cast(value as decimal(38, 0)) / 1e18 as fee,
    '{{ symbol }}' as symbol,
    substr(input, 17, 20) as provider,
    block_time,
    block_date,
    "from" as caller
FROM
    {{source(blockchain, 'traces')}}
WHERE
    contains(
        array[
            0x93cbf217   -- request(address,bytes32,bool)
            , 0x19cb825f -- requestWithCallback(address,bytes32)
            , 0x7b43155d -- requestV2()
            , 0x0bed189f -- requestV2(uint32 gasLimit)
            , 0x0e33da29 -- requestV2(address provider, uint32 gasLimit)
        ],
        substr(input, 1, 4)
    )
    and tx_success
    and call_type = 'call'
    and success
    and to = {{ entropy_address }}
{% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
{% endif %}
{% endmacro %}

{% macro pyth_entropy_request_zksync_chain(blockchain, symbol, entropy_address) %}
SELECT
    '{{ blockchain }}' as blockchain,
    tx_hash,
    from_big_endian_64(substr(output, 25, 8)) as assigned_sequence_number,
    cast(value as decimal(38, 0)) / 1e18 as fee,
    '{{ symbol }} 'as symbol,
    substr(input, 17, 20) as provider,
    block_time,
    block_date,
    "from" as caller
FROM
    {{source(blockchain, 'traces')}}
WHERE
    contains(
        array[
            0x93cbf217   -- request(address,bytes32,bool)
            , 0x19cb825f -- requestWithCallback(address,bytes32)
            , 0x7b43155d -- requestV2()
            , 0x0bed189f -- requestV2(uint32 gasLimit)
            , 0x0e33da29 -- requestV2(address provider, uint32 gasLimit)
        ],
        substr(input, 1, 4)
    )
    and tx_success
    and call_type = 'call'
    and success
    and to = {{ entropy_address }}
    and element_at(trace_address, -1) = 0
{% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
{% endif %}
{% endmacro %}
-- Zksync does a weird thing where native ether need to be handled by system contract
-- before it called the contract again??? this filter out to be the second call
-- https://docs.zksync.io/zksync-protocol/differences/evm-instructions#call-staticcall-delegatecall