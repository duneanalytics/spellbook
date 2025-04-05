{{
    config(
        materialized='incremental',
        schema='safe_celo',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(blockchains = \'["celo"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["danielpartida", "hosuke"]\') }}'
    )
}}

{{ safe_transactions('celo', '2021-06-20') }}
