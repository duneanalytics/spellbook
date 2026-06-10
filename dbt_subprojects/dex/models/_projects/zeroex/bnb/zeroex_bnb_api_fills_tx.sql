{{
    config(
        schema = 'zeroex_bnb',
        alias = 'api_fills_tx',
        materialized = 'incremental',
        partition_by = ['block_month'],
        unique_key = ['block_month', 'tx_hash'],
        on_schema_change = 'sync_all_columns',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Shared staging model for the 0x API affiliate/tracker scan on BNB Chain.
-- Materializing this once breaks the CTE inlining that previously re-scanned
-- bnb.traces many times per zeroex_bnb_api_fills build (the calldata
-- affiliate-selector search on input is non-pushable).
-- block_time is carried (max over the tx) purely so the staging table can be
-- partitioned/incremental; downstream reads only (tx_hash, affiliate_address).
select
    api_fills_tx.*,
    cast(date_trunc('month', block_time) as date) as block_month
from (
    SELECT tx_hash,
           max(affiliate_address) as affiliate_address,
           max(block_time) as block_time
    FROM (
        SELECT
            v3.evt_tx_hash AS tx_hash,
            CASE
                WHEN takerAddress = 0x63305728359c088a52b0b0eeec235db4d31a67fc THEN takerAddress
                ELSE CAST(NULL as varbinary)
            END AS affiliate_address,
            v3.evt_block_time AS block_time
        FROM
            {{ source('zeroex_v2_bnb', 'Exchange_evt_Fill') }} v3
        WHERE
            ( -- nuo
                v3.takerAddress = 0x63305728359c088a52b0b0eeec235db4d31a67fc
                OR -- contains a bridge order
                (
                    v3.feeRecipientAddress = 0x1000000000000000000000000000000000000011
                    AND bytearray_substring(v3.makerAssetData, 1, 4) = 0xdc1600f3
                )
            )
            {% if is_incremental() %}
            AND {{ incremental_predicate('evt_block_time') }}
            {% else %}
            AND evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
            {% endif %}

        UNION ALL

        SELECT
            tr.tx_hash AS tx_hash,
            CASE
                WHEN bytearray_position(INPUT, 0x869584cd ) <> 0
                    THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0x869584cd) + 16) FOR 20)
                WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0
                    THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0xfbc019a7 ) + 16) FOR 20)
            END AS affiliate_address,
            tr.block_time AS block_time
        FROM {{ source('bnb', 'traces') }} tr
        WHERE tr.to IN (
                -- exchange contract
                0x61935cbdd02287b511119ddb11aeb42f1593b7ef,
                -- forwarder addresses
                0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5,
                0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be,
                0x4ef40d1bf0983899892946830abf99eca2dbc5ce,
                -- exchange proxy
                0xdef1c0ded9bec7f1a1670819833240f027b25eff
                )
                AND (
                    bytearray_position(INPUT, 0x869584cd ) <> 0
                    OR bytearray_position(INPUT, 0xfbc019a7 ) <> 0
                )
                {% if is_incremental() %}
                AND {{ incremental_predicate('block_time') }}
                {% else %}
                AND block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
                {% endif %}
    ) temp
    group by tx_hash
) as api_fills_tx
