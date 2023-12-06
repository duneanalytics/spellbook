{{
    config(
        alias = 'eth',

        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}

{{transfers_eth(
    blockchain='base'
)}}
