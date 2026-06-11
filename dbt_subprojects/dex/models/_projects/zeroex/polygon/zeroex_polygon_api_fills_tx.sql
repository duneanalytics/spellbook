{{
    config(
        schema = 'zeroex_polygon',
        alias = 'api_fills_tx',
        materialized = 'incremental',
        partition_by = ['block_month'],
        unique_key = ['block_month', 'tx_hash', 'block_number'],
        on_schema_change = 'sync_all_columns',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}
{%- if target.name == 'ci' -%}
{%- set zeroex_v3_start_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=14)).strftime('%Y-%m-%d') -%}
{%- set zeroex_v4_start_date = zeroex_v3_start_date -%}
{%- endif -%}

-- Shared staging model for the 0x API affiliate/tracker scan on Polygon.
-- Materializing this once breaks the CTE inlining that previously re-scanned
-- polygon.traces many times per zeroex_polygon_api_fills build (the calldata
-- affiliate-selector search on input is non-pushable).
select
    api_fills_tx.*,
    cast(date_trunc('month', block_time) as date) as block_month
from (
    SELECT distinct
             tr.tx_hash,
                       max(CASE
                            WHEN bytearray_position(INPUT, 0x869584cd ) <> 0 THEN SUBSTRING(INPUT
                                                                                   FROM (bytearray_position(INPUT, 0x869584cd) + 16)
                                                                                   FOR 20)
                            WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0 THEN SUBSTRING(INPUT
                                                                                   FROM (bytearray_position(INPUT, 0xfbc019a7 ) + 16)
                                                                                   FOR 20)
                        END) AS affiliate_address,
            tr.block_number as block_number,
            tr.block_time as block_time
        FROM {{ source('polygon', 'traces') }} tr
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
                {% endif %}
                {% if not is_incremental() %}
                AND block_time >= cast('{{zeroex_v3_start_date}}' as date)
                {% endif %}
            group by tr.tx_hash, tr.block_number, tr.block_time
) as api_fills_tx
