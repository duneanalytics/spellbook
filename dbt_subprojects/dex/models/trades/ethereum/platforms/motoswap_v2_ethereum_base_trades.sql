{{
    config(
        schema = 'motoswap_v2_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Motoswap is a Uniswap V2 fork. The factory (ERC-1967 proxy) emits the standard Uniswap V2 PairCreated event,
-- and every pair emits the standard Uniswap V2 Swap event, so the rows are decoded from ethereum.logs by topic0
-- and fed to the shared uniswap_compatible_v2_trades macro.
-- Factory proxy: 0x81c9cbc47d700da1777abd831d8da3f526dfae24, deployed at block 26075263 on 2026-09-28.

{% set motoswap_start_date = '2026-09-28' %}
{% set motoswap_factory = '0x81c9cbc47d700da1777abd831d8da3f526dfae24' %}

{%- set Factory_evt_PairCreated -%}
(
    SELECT
        bytearray_substring(l.topic1, 13, 20) AS token0
        , bytearray_substring(l.topic2, 13, 20) AS token1
        , bytearray_substring(l.data, 13, 20) AS pair
    FROM {{ source('ethereum', 'logs') }} l
    WHERE l.contract_address = {{ motoswap_factory }}
        AND l.topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 -- PairCreated
        AND l.block_date >= DATE '{{ motoswap_start_date }}'
)
{%- endset -%}

{%- set Pair_evt_Swap -%}
(
    SELECT
        l.contract_address
        , l.block_time AS evt_block_time
        , l.block_number AS evt_block_number
        , l.tx_hash AS evt_tx_hash
        , l.index AS evt_index
        , bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) AS amount0In
        , bytearray_to_uint256(bytearray_substring(l.data, 33, 32)) AS amount1In
        , bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) AS amount0Out
        , bytearray_to_uint256(bytearray_substring(l.data, 97, 32)) AS amount1Out
        , bytearray_substring(l.topic2, 13, 20) AS "to"
    FROM {{ source('ethereum', 'logs') }} l
    WHERE l.topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 -- Swap
        AND l.block_date >= DATE '{{ motoswap_start_date }}'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('l.block_time') }}
        {%- endif %}
)
{%- endset -%}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'ethereum',
        project = 'motoswap',
        version = '2',
        Pair_evt_Swap = Pair_evt_Swap,
        Factory_evt_PairCreated = Factory_evt_PairCreated
    )
}}
